/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

/**
 * 从Processor的处理来看，如果没有读取到消息，会判断是否是PushConsumer
 * （参数hasSuspendFlag）以及是否允许挂起（参数brokerAllowSuspend），
 * 如果是的话，则将请求挂起，方式就是封装成PullRequest
 * 提交给PullRequestHoldService。所以我们这里看看它是如何挂起请求处理的：
 */
public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    /**
     * 拉取消息请求集合
     */
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 添加拉取消息挂起请求
     * @param topic 主题
     * @param queueId 队列编号
     * @param pullRequest 拉取消息请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        /**
         * pullRequest被放入一个以topic+queue为key的Map中， value为一个队列ManyPullRequest
         */
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    /**
     * 根据主题 + 队列编号 创建唯一标示
     * @param topic 主题
     * @param queueId 队列编号
     * @return key
     */
    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                /**
                 * 根据 长轮训 还是 短轮训 设置不同的等待时间
                 * 是否开启长轮询, 长轮询等待5秒，短轮询等待1秒  如何过被通知有消息了就不会等，没有就等待
                 */
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }
                /**
                 * 检查挂起请求是否有需要通知的
                 */
                long beginLockTimestamp = this.systemClock.now();
                /**
                 * 时间到了，或者有消息了就去通知消息消息到达
                 */
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 遍历挂起请求，检查是否有需要通知的请求。
     */
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    /**
                     * 通知消息到达，这个时候可能有，也可能没有
                     */
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    /**
     * 检查是否有需要通知的请求
     * @param topic
     * @param queueId
     * @param maxOffset 消费队列最大offset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * 检查是否有需要通知的请求
     * @param topic 主题
     * @param queueId 队列编号
     * @param maxOffset 消费队列最大offset
     * @param tagsCode 过滤tagsCode
     * @param msgStoreTime
     * @param filterBitMap
     * @param properties
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        /**
         * 拿到topic+queueId对应的所有PullRequest
         */
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            /**
             * clone等待的List，同时会清空等待列表
             */
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>(); // 不符合唤醒的请求数组

                for (PullRequest request : requestList) {
                    /**
                     * 判断等待的时间内有没有新的消息进来
                     */
                    /**
                     * 如果 maxOffset 过小，则重新读取一次。
                     */
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }
                    /**
                     * 有新的匹配消息，唤醒请求，即再次拉取消息。
                     * 有消息到来
                     */
                    if (newestOffset > request.getPullFromThisOffset()) {
                        /**
                         * 判断消息是否符合过滤条件
                         */
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }
                        /**
                         * 匹配到消息，重新通过PullRequestProcessor执行消息读取
                         */
                        if (match) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }
                    /**
                     * 超过挂起时间，唤醒请求，即再次拉取消息。
                     *
                     * 如果requst等待超时，无论前一步是否符合条件，肯定会发给processor处理
                     */
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }
                    /**
                     * 不符合再次拉取的请求，再次添加回去
                     * 未超时和没有匹配到消息的request，重新放入队列等待
                     */
                    replayList.add(request);
                }
                /**
                 * 添加回去
                 */
                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
