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

package com.alibaba.nacos.naming.misc;

/**
 * Synchronizer.
 *
 * @author nacos
 */
public interface Synchronizer {
    
    /**
     * Send message to server.
     * 发送消息到别的server
     * @param serverIp target server address
     * @param msg      message to send
     */
    void send(String serverIp, Message msg);
    
    /**
     * Get message from server using message key.
     * 用消息key从别的server获取message
     * @param serverIp source server address
     * @param key      message key 所谓的key只是一个拼接而已，真不高级 namespaceId##serviceName
     * @return message
     */
    Message get(String serverIp, String key);
}
