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
