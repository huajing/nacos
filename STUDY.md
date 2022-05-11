# IDEA本地启动
nacos-consle -> Nacos (vm参数：-Dnacos.standalone=true)


# Mysql
集群启动要使用mysql

### docker 启动mysql
参考url: https://www.cnblogs.com/nuo010/p/15787489.html

# client-api使用

## NacosConfigService
配置管理的api

## NacosNamingService
服务管理的api

## 一点心得
优秀的源码每个方法都是有重要逻辑的，找重点不要每行都看，一般来很多是参数校验，构造
然后走得核心逻辑，只需要关注核心逻辑既可

## 如何开始
1、从客户端如何注册到服务端开始，但下一步存在的问题就是一些功能跟不到，有些理线程调度执行的

2、找到统一管理的线程池，这样就可以知道server->server, server->client之前的通信问题，
详见：com.alibaba.nacos.naming.misc.GlobalExecutor

## 概念
service --> cluster --> instance
