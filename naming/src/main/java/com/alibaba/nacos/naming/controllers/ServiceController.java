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

package com.alibaba.nacos.naming.controllers;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.Selector;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.model.RestResultUtils;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.NumberUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.core.ServiceOperator;
import com.alibaba.nacos.naming.core.ServiceOperatorV1Impl;
import com.alibaba.nacos.naming.core.ServiceOperatorV2Impl;
import com.alibaba.nacos.naming.core.SubscribeManager;
import com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadata;
import com.alibaba.nacos.naming.core.v2.upgrade.UpgradeJudgement;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.selector.NoneSelector;
import com.alibaba.nacos.naming.selector.SelectorManager;
import com.alibaba.nacos.naming.utils.ServiceUtil;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Service operation controller.
 *
 * @author nkorange
 *
 * /v1/ns/instance
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + UtilsAndCommons.NACOS_NAMING_SERVICE_CONTEXT)
public class ServiceController {
    
    @Autowired
    protected ServiceManager serviceManager;
    
    @Autowired
    private ServerMemberManager memberManager;
    
    @Autowired
    private SubscribeManager subscribeManager;
    
    @Autowired
    private ServiceOperatorV1Impl serviceOperatorV1;
    
    @Autowired
    private ServiceOperatorV2Impl serviceOperatorV2;
    
    @Autowired
    private UpgradeJudgement upgradeJudgement;
    
    @Autowired
    private SelectorManager selectorManager;
    
    /**
     * Create a new service. This API will create a persistence service.
     *
     * @param namespaceId      namespace id
     * @param serviceName      service name
     * @param protectThreshold protect threshold
     * @param metadata         service metadata
     * @param selector         selector
     * @return 'ok' if success
     * @throws Exception exception
     */
    @PostMapping
    @Secured(action = ActionTypes.WRITE)
    public String create(@RequestParam(defaultValue = Constants.DEFAULT_NAMESPACE_ID) String namespaceId,
            @RequestParam String serviceName,
            @RequestParam(required = false, defaultValue = "0.0F") float protectThreshold,
            @RequestParam(defaultValue = StringUtils.EMPTY) String metadata,
            @RequestParam(defaultValue = StringUtils.EMPTY) String selector) throws Exception {
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        serviceMetadata.setProtectThreshold(protectThreshold);
        serviceMetadata.setSelector(parseSelector(selector));
        serviceMetadata.setExtendData(UtilsAndCommons.parseMetadata(metadata));
        serviceMetadata.setEphemeral(false);
        getServiceOperator().create(namespaceId, serviceName, serviceMetadata);
        return "ok";
    }
    
    /**
     * Remove service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @return 'ok' if success
     * @throws Exception exception
     */
    @DeleteMapping
    @Secured(action = ActionTypes.WRITE)
    public String remove(@RequestParam(defaultValue = Constants.DEFAULT_NAMESPACE_ID) String namespaceId,
            @RequestParam String serviceName) throws Exception {
        
        getServiceOperator().delete(namespaceId, serviceName);
        return "ok";
    }
    
    /**
     * Get detail of service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @return detail information of service
     * @throws NacosException nacos exception
     */
    @GetMapping
    @Secured(action = ActionTypes.READ)
    public ObjectNode detail(@RequestParam(defaultValue = Constants.DEFAULT_NAMESPACE_ID) String namespaceId,
            @RequestParam String serviceName) throws NacosException {
        return getServiceOperator().queryService(namespaceId, serviceName);
    }
    
    /**
     * List all service names.
     * /v1/ns/instance/list
     * @param request http request
     * @return all service names
     * @throws Exception exception
     */
    @GetMapping("/list")
    @Secured(action = ActionTypes.READ)
    public ObjectNode list(HttpServletRequest request) throws Exception {
        final int pageNo = NumberUtils.toInt(WebUtils.required(request, "pageNo"));
        final int pageSize = NumberUtils.toInt(WebUtils.required(request, "pageSize"));
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String groupName = WebUtils.optional(request, CommonParams.GROUP_NAME, Constants.DEFAULT_GROUP);
        String selectorString = WebUtils.optional(request, "selector", StringUtils.EMPTY);

        //返回数据
        ObjectNode result = JacksonUtils.createEmptyJsonNode();

        //核心代码：
        Collection<String> serviceNameList = getServiceOperator()//v1和v2的实现不同
                .listService(namespaceId, groupName, selectorString);

        //返回数据设置
        result.put("count", serviceNameList.size());
        result.replace("doms",
                JacksonUtils.transferToJsonNode(ServiceUtil.pageServiceName(pageNo, pageSize, serviceNameList)));
        return result;
        
    }
    
    /**
     * Update service.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception exception
     */
    @PutMapping
    @Secured(action = ActionTypes.WRITE)
    public String update(HttpServletRequest request) throws Exception {
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        serviceMetadata.setProtectThreshold(NumberUtils.toFloat(WebUtils.required(request, "protectThreshold")));
        serviceMetadata.setExtendData(
                UtilsAndCommons.parseMetadata(WebUtils.optional(request, "metadata", StringUtils.EMPTY)));
        serviceMetadata.setSelector(parseSelector(WebUtils.optional(request, "selector", StringUtils.EMPTY)));
        com.alibaba.nacos.naming.core.v2.pojo.Service service = com.alibaba.nacos.naming.core.v2.pojo.Service
                .newService(namespaceId, NamingUtils.getGroupName(serviceName),
                        NamingUtils.getServiceName(serviceName));
        getServiceOperator().update(service, serviceMetadata);
        return "ok";
    }
    
    /**
     * Search service names.
     *
     * @param namespaceId     namespace
     * @param expr            search pattern
     * @param responsibleOnly whether only search responsible service
     * @return search result
     */
    @RequestMapping("/names")
    @Secured(action = ActionTypes.READ)
    public ObjectNode searchService(@RequestParam(defaultValue = StringUtils.EMPTY) String namespaceId,
            @RequestParam(defaultValue = StringUtils.EMPTY) String expr,
            @RequestParam(required = false) boolean responsibleOnly) throws NacosException {
        Map<String, Collection<String>> serviceNameMap = new HashMap<>(16);
        int totalCount = 0;
        if (StringUtils.isNotBlank(namespaceId)) {
            Collection<String> names = getServiceOperator().searchServiceName(namespaceId, expr, responsibleOnly);
            serviceNameMap.put(namespaceId, names);
            totalCount = names.size();
        } else {
            for (String each : getServiceOperator().listAllNamespace()) {
                Collection<String> names = getServiceOperator().searchServiceName(each, expr, responsibleOnly);
                serviceNameMap.put(each, names);
                totalCount += names.size();
            }
        }
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        result.replace("services", JacksonUtils.transferToJsonNode(serviceNameMap));
        result.put("count", totalCount);
        return result;
    }
    
    /**
     * Check service status whether latest.
     * 检查服务的状态是否是最新的
     * 假设是从结点向leader调用？？应该是server之间的调用
     * @param request http request
     * @return 'ok' if service status if latest, otherwise 'fail' or exception
     * @throws Exception exception
     * @deprecated will removed after v2.1
     */
    @PostMapping("/status")
    @Deprecated
    public String serviceStatus(HttpServletRequest request) throws Exception {
        
        String entity = IoUtils.toString(request.getInputStream(), "UTF-8");
        String value = URLDecoder.decode(entity, "UTF-8");
        JsonNode json = JacksonUtils.toObj(value);

        //从请求中解析数据
        //format: service1@@checksum@@@service2@@checksum
        String statuses = json.get("statuses").asText();
        //客户端的ip,也就是请求机器的ip地址
        String serverIp = json.get("clientIP").asText();

        //serverIp在集群中
        if (!memberManager.hasMember(serverIp)) {
            throw new NacosException(NacosException.INVALID_PARAM, "ip: " + serverIp + " is not in serverlist");
        }
        
        try {
            //将请请求中的statuses转化为检验码对象
            ServiceManager.ServiceChecksum checksums = JacksonUtils
                    .toObj(statuses, ServiceManager.ServiceChecksum.class);
            if (checksums == null) {
                Loggers.SRV_LOG.warn("[DOMAIN-STATUS] receive malformed data: null");
                //请求中的数据不对，直接返回错误
                return "fail";
            }
            
            for (Map.Entry<String, String> entry : checksums.serviceName2Checksum.entrySet()) {
                if (entry == null || StringUtils.isEmpty(entry.getKey()) || StringUtils.isEmpty(entry.getValue())) {
                    continue;
                }
                //主要数据1，服务名
                String serviceName = entry.getKey();
                //主要数据2，校验码
                String checksum = entry.getValue();

                //获取取指向命名空间下服务名的服务
                Service service = serviceManager.getService(checksums.namespaceId, serviceName);
                
                if (service == null) {
                    continue;
                }

                //重新计算本结点的校验码
                service.recalculateChecksum();

                //比较请求结点与本地结点校验码是否一致
                if (!checksum.equals(service.getChecksum())) {
                    if (Loggers.SRV_LOG.isDebugEnabled()) {
                        Loggers.SRV_LOG.debug("checksum of {} is not consistent, remote: {}, checksum: {}, local: {}",
                                serviceName, serverIp, checksum, service.getChecksum());
                    }
                    //不一致就加入到阻塞队列中启动推送任务，ServiceManager->UpdatedServiceProcessor->run()
                    serviceManager.addUpdatedServiceToQueue(checksums.namespaceId, serviceName, serverIp, checksum);
                }
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("[DOMAIN-STATUS] receive malformed data: " + statuses, e);
        }
        //执行成功返回 "ok" 就可以
        return "ok";
    }
    
    /**
     * Get checksum of one service.
     *
     * @param request http request
     * @return checksum of one service
     * @throws Exception exception
     * @deprecated will removed after v2.1
     */
    @PutMapping("/checksum")
    @Deprecated
    public ObjectNode checksum(HttpServletRequest request) throws NacosException {
        
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        Service service = serviceManager.getService(namespaceId, serviceName);
        
        serviceManager.checkServiceIsNull(service, namespaceId, serviceName);
        
        service.recalculateChecksum();
        
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        
        result.put("checksum", service.getChecksum());
        
        return result;
    }
    
    /**
     * get subscriber list.
     * 分页获取订阅列表
     * @param request http request
     * @return Jackson object node
     */
    @GetMapping("/subscribers")
    @Secured(action = ActionTypes.READ)
    public ObjectNode subscribers(HttpServletRequest request) {
        
        int pageNo = NumberUtils.toInt(WebUtils.optional(request, "pageNo", "1"));
        int pageSize = NumberUtils.toInt(WebUtils.optional(request, "pageSize", "1000"));
        
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        boolean aggregation = Boolean
                .parseBoolean(WebUtils.optional(request, "aggregation", String.valueOf(Boolean.TRUE)));
        
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        
        try {
            List<Subscriber> subscribers = subscribeManager.getSubscribers(serviceName, namespaceId, aggregation);
            
            int start = (pageNo - 1) * pageSize;
            if (start < 0) {
                start = 0;
            }
            
            int end = start + pageSize;
            int count = subscribers.size();
            if (end > count) {
                end = count;
            }
            
            result.replace("subscribers", JacksonUtils.transferToJsonNode(subscribers.subList(start, end)));
            result.put("count", count);
            
            return result;
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("query subscribers failed!", e);
            result.replace("subscribers", JacksonUtils.createEmptyArrayNode());
            result.put("count", 0);
            return result;
        }
    }
    
    /**
     * Get all {@link Selector} types.
     *
     * @return {@link Selector} types.
     */
    @GetMapping("/selector/types")
    public RestResult<List<String>> listSelectorTypes() {
        return RestResultUtils.success(selectorManager.getAllSelectorTypes());
    }
    
    private Selector parseSelector(String selectorJsonString) throws Exception {
        if (StringUtils.isBlank(selectorJsonString)) {
            return new NoneSelector();
        }
        
        JsonNode selectorJson = JacksonUtils.toObj(URLDecoder.decode(selectorJsonString, "UTF-8"));
        String type = Optional.ofNullable(selectorJson.get("type"))
                .orElseThrow(() -> new NacosException(NacosException.INVALID_PARAM, "not match any type of selector!"))
                .asText();
        String expression = Optional.ofNullable(selectorJson.get("expression")).map(JsonNode::asText).orElse(null);
        Selector selector = selectorManager.parseSelector(type, expression);
        if (Objects.isNull(selector)) {
            throw new NacosException(NacosException.INVALID_PARAM, "not match any type of selector!");
        }
        return selector;
    }
    
    private ServiceOperator getServiceOperator() {
        return upgradeJudgement.isUseGrpcFeatures() ? serviceOperatorV2 : serviceOperatorV1;
    }
}
