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

import com.alibaba.nacos.naming.healthcheck.heartbeat.BeatProcessor;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Thread to update ephemeral instance triggered by client beat for v1.x.
 * 线程任务处理心跳
 * @author nkorange
 */
public class ClientBeatProcessor implements BeatProcessor {
    
    private RsInfo rsInfo;
    
    private Service service;
    
    @JsonIgnore
    public UdpPushService getPushService() {
        return ApplicationUtils.getBean(UdpPushService.class);
    }
    
    public RsInfo getRsInfo() {
        return rsInfo;
    }
    
    public void setRsInfo(RsInfo rsInfo) {
        this.rsInfo = rsInfo;
    }
    
    public Service getService() {
        return service;
    }
    
    public void setService(Service service) {
        this.service = service;
    }
    
    @Override
    public void run() {
        Service service = this.service;
        if (Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[CLIENT-BEAT] processing beat: {}", rsInfo.toString());
        }
        
        String ip = rsInfo.getIp();
        String clusterName = rsInfo.getCluster();
        int port = rsInfo.getPort();
        Cluster cluster = service.getClusterMap().get(clusterName);
        List<Instance> instances = cluster.allIPs(true);

        //遍历所有实例
        for (Instance instance : instances) {

            //找到当刷新心跳的实例
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[CLIENT-BEAT] refresh beat: {}", rsInfo.toString());
                }
                //设置当前时间戳
                instance.setLastBeat(System.currentTimeMillis());
                if (!instance.isMarked() && !instance.isHealthy()) {
                    //实例重新设置为健康状态
                    instance.setHealthy(true);
                    Loggers.EVT_LOG
                            .info("service: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: client beat ok",
                                    cluster.getService().getName(), ip, port, cluster.getName(),
                                    UtilsAndCommons.LOCALHOST_SITE);

                    //核心逻辑：UdpPushService，使用ApplicationContext发布事件通知
                    getPushService().serviceChanged(service);
                }
            }
        }
    }
}
