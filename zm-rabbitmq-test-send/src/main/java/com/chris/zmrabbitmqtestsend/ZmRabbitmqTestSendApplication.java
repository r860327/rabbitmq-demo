package com.chris.zmrabbitmqtestsend;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;

@SpringBootApplication
@Slf4j
public class ZmRabbitmqTestSendApplication implements ApplicationRunner {
    private final static String QUEUE_NAME = "hello";

    @Autowired
    DelayMsgWithDLQSend delayMsgWithDLQSend;

    @Autowired
    DelayMsgWithPluginSend delayMsgWithPluginSend;

    public static void main(String[] args) {
        SpringApplication.run(ZmRabbitmqTestSendApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        delayMsgWithPluginSend.sendDelayMsg("message 1", 40000L);
        Thread.sleep(5000L);
        delayMsgWithPluginSend.sendDelayMsg("message 2", 10000L);
        Thread.sleep(5000L);
        delayMsgWithPluginSend.sendDelayMsg("message 3", 10000L);
    }

    private void basicSend() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            for (int i = 0; i < 100; i++) {
                String message = "hello world!.......... " + i;
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                log.info("[x] sent '{}'", message);
                Thread.sleep(1000);
            }
        }
    }

    private void pubSend() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

//            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.exchangeDeclare("logs", "fanout");

            for (int i = 0; i < 100; i++) {
                String message = "hello world!.......... " + i;
                channel.basicPublish("logs", "", null, message.getBytes());
                log.info("[x] sent '{}'", message);
                Thread.sleep(1000);
            }
        }
    }

    private void routingSend() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

//            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.exchangeDeclare("direct_logs", "direct");

            for (int i = 0; i < 100; i++) {
                String message = "hello world!.......... " + i;
                if (i % 3 == 0) {
                    channel.basicPublish("direct_logs", "red", null, message.getBytes());
                } else {
                    channel.basicPublish("direct_logs", "yellow", null, message.getBytes());
                }
                log.info("[x] sent '{}'", message);
                Thread.sleep(1000);
            }
        }
    }

    private void topicSend() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        String [] colors = {"red", "yellow", "blue", "green"};
        String [] sizes = {"big", "small", "middle"};

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

//            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.exchangeDeclare("topic_logs", "topic");

            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < 5; j++) {
                    String routingKey = colors[i % 4] + "." + sizes[j % 3];

                    String message = "hello world!.......... " + i + "-" + j + "-" + routingKey;
                    if (i % 3 == 0) {
                        channel.basicPublish("topic_logs", routingKey, null, message.getBytes());
                    } else {
                        channel.basicPublish("topic_logs", routingKey, null, message.getBytes());
                    }
                    log.info("[x] sent '{}'", message);
                    Thread.sleep(1000);
                }
            }
        }
    }

    // rpc send demo
    static class RPCClient implements AutoCloseable {
        private final Connection connection;
        private final Channel channel;
        private String RPC_QUEUE_NAME = "rpc_queue";

        public RPCClient() throws IOException, TimeoutException {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(32773);

            connection = factory.newConnection();
            channel = connection.createChannel();
        }


        private String rpcCall(String message) throws Exception {
            String corrId = UUID.randomUUID().toString();

            String replyQueneName = channel.queueDeclare().getQueue();
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(corrId)
                    .replyTo(replyQueneName)
                    .build();


            channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));

            final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);
            String ctag = channel.basicConsume(replyQueneName, true, ((consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                    response.offer(new String(delivery.getBody(), "UTF-8"));
                }
            }), consumerTag -> {
            });

            String result = response.take();
            channel.basicCancel(ctag);
            return result;
        }

        @Override
        public void close() throws Exception {
            connection.close();
        }
    }

    private void rpcSend() {
        try(RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 100; i++) {
                String iStr = Integer.toString(i);
                log.info(" [x] Reauesting fib({})", iStr);
                String response = fibonacciRpc.rpcCall(iStr);
                log.info(" [.] Got '{}'", response);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }


}
