package rocketmq;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * Created by erik_mac on 11/25/15.
 */
public class Producer {
    private DefaultMQProducer producer;

    public Producer(String addr) {
        producer = new DefaultMQProducer("Producer");
        producer.setNamesrvAddr(addr);
    }

    public void startMQ() throws MQClientException {
        producer.start();
    }

    public void shutdownMQ(){
        producer.shutdown();
    }

    public SendResult sendMessage(String info)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message msg = new Message("PushTopic",
                "push",
                "1",
                info.getBytes());
         return producer.send(msg);
    }

}
