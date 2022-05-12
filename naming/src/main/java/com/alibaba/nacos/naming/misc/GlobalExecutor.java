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

import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.core.utils.ClassUtils;
import com.alibaba.nacos.naming.NamingApp;
import com.alibaba.nacos.sys.env.EnvUtil;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Global executor for naming.
 * 给naming使用的全局线程池，这个类看起来还是比较重要的外部分服务的交互的一些线程池都在这里
 * 看明白这里的逻辑，整个nacos的运行也了解差不多了，所以我要从这里开始
 * @author nacos
 */
@SuppressWarnings({"checkstyle:indentation", "PMD.ThreadPoolCreationRule"})
public class GlobalExecutor {
    
    public static final long HEARTBEAT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5L);
    
    public static final long LEADER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15L);

    //每一个结点能发起leader投票的一个延迟随机，看起来的意思是减少同一时间发起投票的可能性，
    //因为投票的时候每个结点都会触发投的，只是大家的延迟时间不一样
    public static final long RANDOM_MS = TimeUnit.SECONDS.toMillis(5L);
    
    public static final long TICK_PERIOD_MS = TimeUnit.MILLISECONDS.toMillis(500L);
    
    private static final long SERVER_STATUS_UPDATE_PERIOD = TimeUnit.SECONDS.toMillis(5);
    
    public static final int DEFAULT_THREAD_COUNT = EnvUtil.getAvailableProcessors(0.5);

    //timer 可以核数的2倍
    private static final ScheduledExecutorService NAMING_TIMER_EXECUTOR = ExecutorFactory.Managed
            .newScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    EnvUtil.getAvailableProcessors(2),
                    new NameThreadFactory("com.alibaba.nacos.naming.timer"));

    /**
     * ServerListManager->ServerStatusReporter->run()
     * 在ServerListManager把自己的状态发送给其它的Server
     * 保持2s一次的固定周期同步给其它的Server
     */
    private static final ScheduledExecutorService SERVER_STATUS_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.status.worker"));
    
    /**
     * Service synchronization executor.
     * 同步执行器 ServiceReporter -> run
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    private static final ScheduledExecutorService SERVICE_SYNCHRONIZATION_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.service.worker")
            );
    
    /**
     * Service update manager executor.
     * ？？不知道 UpdatedServiceProcessor -> run()
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    public static final ScheduledExecutorService SERVICE_UPDATE_MANAGER_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.service.update.processor"));
    
    /**
     * thread pool that processes getting service detail from other server asynchronously.
     * 从其它的服务端异步获取service的详情，看样子是server->server同步数据的线程
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    private static final ExecutorService SERVICE_UPDATE_EXECUTOR = ExecutorFactory.Managed
            .newFixedExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    2,
                    new NameThreadFactory("com.alibaba.nacos.naming.service.update.http.handler"));
    
    /**
     * Empty service auto clean executor.
     * 自动清理？？
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    private static final ScheduledExecutorService EMPTY_SERVICE_AUTO_CLEAN_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.service.empty.auto-clean"));

    /**
     * distro.notifier
     */
    private static final ScheduledExecutorService DISTRO_NOTIFY_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.distro.notifier"));

    /**
     * health-check.notifier,启动了没有使用到
     */
    private static final ScheduledExecutorService NAMING_HEALTH_CHECK_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.health-check.notifier"));

    /**
     * mysql.checker，没有看明白为啥要这个
     */
    private static final ExecutorService MYSQL_CHECK_EXECUTOR = ExecutorFactory.Managed
            .newFixedExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    DEFAULT_THREAD_COUNT, //一半核数
                    new NameThreadFactory("com.alibaba.nacos.naming.mysql.checker"));

    /**
     * supersense.checker
     */
    private static final ScheduledExecutorService TCP_SUPER_SENSE_EXECUTOR = ExecutorFactory.Managed
            .newScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    DEFAULT_THREAD_COUNT,//一半核数
                    new NameThreadFactory("com.alibaba.nacos.naming.supersense.checker"));

    /**
     * tcp.check.worker
     */
    private static final ExecutorService TCP_CHECK_EXECUTOR = ExecutorFactory.Managed
            .newFixedExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class), 2,
                    new NameThreadFactory("com.alibaba.nacos.naming.tcp.check.worker"));

    /**
     * naming.health
     */
    private static final ScheduledExecutorService NAMING_HEALTH_EXECUTOR = ExecutorFactory.Managed
            .newScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    Integer.max(Integer.getInteger("com.alibaba.nacos.naming.health.thread.num", DEFAULT_THREAD_COUNT), 1),
                    new NameThreadFactory("com.alibaba.nacos.naming.health"));

    /**
     * push.retransmitter
     */
    private static final ScheduledExecutorService RETRANSMITTER_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.push.retransmitter"));

    /**
     * push.udpSender
     */
    private static final ScheduledExecutorService UDP_SENDER_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.push.udpSender"));

    /**
     * nacos-server-performance
     */
    private static final ScheduledExecutorService SERVER_PERFORMANCE_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.nacos-server-performance"));

    /**
     * remote-connection-manager
     */
    private static final ScheduledExecutorService EXPIRED_CLIENT_CLEANER_EXECUTOR = ExecutorFactory.Managed
            .newSingleScheduledExecutorService(
                    ClassUtils.getCanonicalName(NamingApp.class),
                    new NameThreadFactory("com.alibaba.nacos.naming.remote-connection-manager"));
    
    private static final ExecutorService PUSH_CALLBACK_EXECUTOR = ExecutorFactory.Managed
            .newSingleExecutorService("Push", new NameThreadFactory("com.alibaba.nacos.naming.push.callback"));
    
    /**
     * Register raft leader election executor.
     * 固定频率选择leader,500ms执行一次
     * @param runnable leader election executor RaftCore -> MasterElection
     * @return future
     * @deprecated will removed with old raft
     */
    @Deprecated
    public static ScheduledFuture registerMasterElection(Runnable runnable) {
        return NAMING_TIMER_EXECUTOR.scheduleAtFixedRate(runnable, 0, TICK_PERIOD_MS, TimeUnit.MILLISECONDS);
    }
    
    public static void registerServerInfoUpdater(Runnable runnable) {
        NAMING_TIMER_EXECUTOR.scheduleAtFixedRate(runnable, 0, 2, TimeUnit.SECONDS);
    }
    
    public static void registerServerStatusReporter(Runnable runnable, long delay) {
        SERVER_STATUS_EXECUTOR.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }
    
    public static void registerServerStatusUpdater(Runnable runnable) {
        NAMING_TIMER_EXECUTOR.scheduleAtFixedRate(runnable, 0, SERVER_STATUS_UPDATE_PERIOD, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Register raft heart beat executor.
     * 注册leader发送心跳的executor
     * @param runnable heart beat executor
     * @return future
     * @deprecated will removed with old raft
     */
    @Deprecated
    public static ScheduledFuture registerHeartbeat(Runnable runnable) {
        return NAMING_TIMER_EXECUTOR.scheduleWithFixedDelay(runnable, 0, TICK_PERIOD_MS, TimeUnit.MILLISECONDS);
    }
    
    public static void scheduleMcpPushTask(Runnable runnable, long initialDelay, long period) {
        NAMING_TIMER_EXECUTOR.scheduleAtFixedRate(runnable, initialDelay, period, TimeUnit.MILLISECONDS);
    }
    
    public static ScheduledFuture submitClusterVersionJudge(Runnable runnable, long delay) {
        return NAMING_TIMER_EXECUTOR.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }
    
    public static void submitDistroNotifyTask(Runnable runnable) {
        DISTRO_NOTIFY_EXECUTOR.submit(runnable);
    }
    
    /**
     * Submit service update for v1.x.
     * 提交service的更新任务，ServiceUpdater->run
     * @param runnable runnable
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    public static void submitServiceUpdate(Runnable runnable) {
        SERVICE_UPDATE_EXECUTOR.execute(runnable);
    }
    
    /**
     * Schedule empty service auto clean for v1.x.
     *
     * @param runnable     runnable
     * @param initialDelay initial delay milliseconds
     * @param period       period between twice clean
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    public static void scheduleServiceAutoClean(Runnable runnable, long initialDelay, long period) {
        EMPTY_SERVICE_AUTO_CLEAN_EXECUTOR.scheduleAtFixedRate(runnable, initialDelay, period, TimeUnit.MILLISECONDS);
    }
    
    /**
     * submitServiceUpdateManager.
     * 监听阻塞队列的service变化，有变化就去更新
     * @param runnable runnable UpdatedServiceProcessor -> run
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    public static void submitServiceUpdateManager(Runnable runnable) {
        SERVICE_UPDATE_MANAGER_EXECUTOR.submit(runnable);
    }
    
    /**
     * scheduleServiceReporter.
     * 定时把本server中的服务报告给别的server，如果有不一致就更新，但到底以谁的为准？？，还需要继续看源码啊
     * @param command command
     * @param delay   delay
     * @param unit    time unit
     * @deprecated will remove in v2.1.x.
     */
    @Deprecated
    public static void scheduleServiceReporter(Runnable command, long delay, TimeUnit unit) {
        SERVICE_SYNCHRONIZATION_EXECUTOR.schedule(command, delay, unit);
    }
    
    public static void scheduleNamingHealthCheck(Runnable command, long delay, TimeUnit unit) {
        NAMING_HEALTH_CHECK_EXECUTOR.schedule(command, delay, unit);
    }
    
    public static void executeMysqlCheckTask(Runnable runnable) {
        MYSQL_CHECK_EXECUTOR.execute(runnable);
    }
    
    public static void submitTcpCheck(Runnable runnable) {
        TCP_CHECK_EXECUTOR.submit(runnable);
    }
    
    public static <T> List<Future<T>> invokeAllTcpSuperSenseTask(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return TCP_SUPER_SENSE_EXECUTOR.invokeAll(tasks);
    }
    
    public static void executeTcpSuperSense(Runnable runnable) {
        TCP_SUPER_SENSE_EXECUTOR.execute(runnable);
    }
    
    public static void scheduleTcpSuperSenseTask(Runnable runnable, long delay, TimeUnit unit) {
        TCP_SUPER_SENSE_EXECUTOR.schedule(runnable, delay, unit);
    }
    
    public static ScheduledFuture<?> scheduleNamingHealth(Runnable command, long delay, TimeUnit unit) {
        return NAMING_HEALTH_EXECUTOR.schedule(command, delay, unit);
    }
    
    public static ScheduledFuture<?> scheduleNamingHealth(Runnable command, long initialDelay, long delay,
            TimeUnit unit) {
        return NAMING_HEALTH_EXECUTOR.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
    
    public static void scheduleRetransmitter(Runnable runnable, long initialDelay, long delay, TimeUnit unit) {
        RETRANSMITTER_EXECUTOR.scheduleWithFixedDelay(runnable, initialDelay, delay, unit);
    }
    
    public static void scheduleRetransmitter(Runnable runnable, long delay, TimeUnit unit) {
        RETRANSMITTER_EXECUTOR.schedule(runnable, delay, unit);
    }
    
    public static ScheduledFuture<?> scheduleUdpSender(Runnable runnable, long delay, TimeUnit unit) {
        return UDP_SENDER_EXECUTOR.schedule(runnable, delay, unit);
    }
    
    public static void scheduleUdpReceiver(Runnable runnable) {
        NAMING_TIMER_EXECUTOR.submit(runnable);
    }
    
    public static void schedulePerformanceLogger(Runnable runnable, long initialDelay, long delay, TimeUnit unit) {
        SERVER_PERFORMANCE_EXECUTOR.scheduleWithFixedDelay(runnable, initialDelay, delay, unit);
    }
    
    public static void scheduleExpiredClientCleaner(Runnable runnable, long initialDelay, long delay, TimeUnit unit) {
        EXPIRED_CLIENT_CLEANER_EXECUTOR.scheduleWithFixedDelay(runnable, initialDelay, delay, unit);
    }
    
    public static ExecutorService getCallbackExecutor() {
        return PUSH_CALLBACK_EXECUTOR;
    }
}
