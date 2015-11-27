package rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;


import java.util.List;

/**
 * Created by erik_mac on 11/25/15.
 */
public class Customer {
    private DefaultMQPushConsumer mqConsumer;

    public Customer(String addr) {
        mqConsumer =
                new DefaultMQPushConsumer("PushConsumer");
        mqConsumer.setNamesrvAddr(addr);
    }

    public void startMQ(MessageListenerConcurrently listener){

        try {
            //订阅PushTopic下Tag为push的消息
            mqConsumer.subscribe("PushTopic", "push");
            //程序第一次启动从消息队列头取数据
            mqConsumer.setConsumeFromWhere(
                    ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            mqConsumer.registerMessageListener(listener);
            mqConsumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
