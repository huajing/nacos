/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.api.naming.pojo.healthcheck.impl.Http;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.http.HttpUtils;
import com.alibaba.nacos.common.http.client.NacosAsyncRestTemplate;
import com.alibaba.nacos.common.http.param.Header;
import com.alibaba.nacos.common.http.param.Query;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.misc.HttpClientManager;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.naming.misc.Loggers.SRV_LOG;

/**
 * HTTP health check processor.
 * http健康检查器
 * @author xuanyin.zy
 */
@Component("httpHealthCheckProcessorV1")
public class HttpHealthCheckProcessor implements HealthCheckProcessor {
    
    public static final String TYPE = "HTTP";
    
    @Autowired
    private SwitchDomain switchDomain;
    
    @Autowired
    private HealthCheckCommon healthCheckCommon;
    
    private static final NacosAsyncRestTemplate ASYNC_REST_TEMPLATE = HttpClientManager.getProcessorNacosAsyncRestTemplate();
    
    @Override
    public String getType() {
        return TYPE;
    }
    
    @Override
    public void process(HealthCheckTask task) {
        //获取集群中的所有实例
        List<Instance> ips = task.getCluster().allIPs(false);
        if (CollectionUtils.isEmpty(ips)) {
            return;
        }
        
        if (!switchDomain.isHealthCheckEnabled()) {
            return;
        }

        //集群
        Cluster cluster = task.getCluster();

        //所有的客户端是否正常
        for (Instance ip : ips) {
            try {

                //标记过
                if (ip.isMarked()) {
                    if (SRV_LOG.isDebugEnabled()) {
                        SRV_LOG.debug("http check, ip is marked as to skip health check, ip: {}" + ip.getIp());
                    }
                    continue;
                }
                
                if (!ip.markChecking()) {
                    SRV_LOG.warn("http check started before last one finished, service: {}:{}:{}",
                            task.getCluster().getService().getName(), task.getCluster().getName(), ip.getIp());
                    
                    healthCheckCommon.reEvaluateCheckRT(task.getCheckRtNormalized() * 2, task,
                            switchDomain.getHttpHealthParams());
                    continue;
                }
                //http 检查器
                Http healthChecker = (Http) cluster.getHealthChecker();
                
                int ckPort = cluster.isUseIPPort4Check() ? ip.getPort() : cluster.getDefCkport();
                URL host = new URL("http://" + ip.getIp() + ":" + ckPort);
                URL target = new URL(host, healthChecker.getPath());
                Map<String, String> customHeaders = healthChecker.getCustomHeaders();
                Header header = Header.newInstance();
                header.addAll(customHeaders);

                //核心方法：见HttpHealthCheckCallback->run()
                ASYNC_REST_TEMPLATE.get(target.toString(), header, Query.EMPTY, String.class,
                        new HttpHealthCheckCallback(ip, task));

                //AtomicInteger incrementAndGet
                MetricsMonitor.getHttpHealthCheckMonitor().incrementAndGet();
            } catch (Throwable e) {
                ip.setCheckRt(switchDomain.getHttpHealthParams().getMax());
                healthCheckCommon.checkFail(ip, task, "http:error:" + e.getMessage());
                healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
                        switchDomain.getHttpHealthParams());
            }
        }
    }
    
    private class HttpHealthCheckCallback implements Callback<String> {
        
        private Instance ip;
        
        private HealthCheckTask task;
        
        private long startTime = System.currentTimeMillis();
        
        public HttpHealthCheckCallback(Instance ip, HealthCheckTask task) {
            this.ip = ip;
            this.task = task;
        }
        
        @Override
        public void onReceive(RestResult<String> result) {
            //chenckRt时间
            ip.setCheckRt(System.currentTimeMillis() - startTime);
            
            int httpCode = result.getCode();

            //正常，HTTP_OK
            if (HttpURLConnection.HTTP_OK == httpCode) {
                healthCheckCommon.checkOK(ip, task, "http:" + httpCode);
                //重新计算响应时间
                healthCheckCommon.reEvaluateCheckRT(System.currentTimeMillis() - startTime, task,
                        switchDomain.getHttpHealthParams());
            //服务繁忙，晚点再次验证
            } else if (HttpURLConnection.HTTP_UNAVAILABLE == httpCode
                    || HttpURLConnection.HTTP_MOVED_TEMP == httpCode) {
                // server is busy, need verification later
                healthCheckCommon.checkFail(ip, task, "http:" + httpCode);
                healthCheckCommon
                        .reEvaluateCheckRT(task.getCheckRtNormalized() * 2, task, switchDomain.getHttpHealthParams());
            //
            } else {
                //probably means the state files has been removed by administrator
                healthCheckCommon.checkFailNow(ip, task, "http:" + httpCode);
                healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
                        switchDomain.getHttpHealthParams());
            }
            
        }
        
        @Override
        public void onError(Throwable t) {
            ip.setCheckRt(System.currentTimeMillis() - startTime);
            
            Throwable cause = t;
            int maxStackDepth = 50;
            for (int deepth = 0; deepth < maxStackDepth && cause != null; deepth++) {
                if (HttpUtils.isTimeoutException(t)) {
                    
                    healthCheckCommon.checkFail(ip, task, "http:timeout:" + cause.getMessage());
                    healthCheckCommon.reEvaluateCheckRT(task.getCheckRtNormalized() * 2, task,
                            switchDomain.getHttpHealthParams());
                    
                    return;
                }
                
                cause = cause.getCause();
            }
            
            // connection error, probably not reachable
            if (t instanceof ConnectException) {
                healthCheckCommon.checkFailNow(ip, task, "http:unable2connect:" + t.getMessage());
                healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
                        switchDomain.getHttpHealthParams());
            } else {
                healthCheckCommon.checkFail(ip, task, "http:error:" + t.getMessage());
                healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
                        switchDomain.getHttpHealthParams());
            }
        }
        
        @Override
        public void onCancel() {
        
        }
    }
}
