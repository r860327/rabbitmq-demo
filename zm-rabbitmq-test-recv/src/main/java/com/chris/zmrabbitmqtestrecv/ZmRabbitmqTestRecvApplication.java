package com.chris.zmrabbitmqtestrecv;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
@Slf4j
public class ZmRabbitmqTestRecvApplication implements ApplicationRunner {
    private final static String QUEUE_NAME = "hello";

    @Autowired
    DelayMsgWithDLQRecv delayMsgWithDLQRecv;

    @Autowired
    DelayMsgWithPluginRecv delayMsgWithPluginRecv;

    public static void main(String[] args) {
        SpringApplication.run(ZmRabbitmqTestRecvApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        delayMsgWithPluginRecv.recv();
    }

    // tutorials 1
    private void basicRecv() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        log.info("[*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            log.info(" [x] Received {}", message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
        });

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    }

    // tutorials 2 work queue
    private void workRecv() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        log.info("[*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(2);

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            log.info(" [x] Received {}", message);
            try {
                doWork(message);
            } catch (InterruptedException e) {
                log.error("", e);
            } finally {
                log.info(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        });

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    }

    private void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }

    // tutorials 3 pub/sub
    private void subRecv() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("logs", "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "logs", "");

        log.info(" [*] Waiting for message. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            log.info(" [x] Received '{}'", message);
        });

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    // tutorials 4 Routing
    private void routingRecv() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("direct_logs", "direct");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "direct_logs", "yellow");
        channel.queueBind(queueName, "direct_logs", "red");

        log.info(" [*] Waiting for message. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            log.info(" [x] Received '{}'", message);
        });

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    // tutorials 5 topic
    private void topicRecv() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("topic_logs", "topic");
        String queueName = channel.queueDeclare().getQueue();
//        channel.queueBind(queueName, "topic_logs", "yellow.big");
//        channel.queueBind(queueName, "topic_logs", "red.*");
        channel.queueBind(queueName, "topic_logs", "#");

        log.info(" [*] Waiting for message. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            log.info(" [x] Received '{}'", message);
        });

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    // tutorials 6 RPC
    private int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n-1) + fib(n-2);
    }

    private void rpcRecv() throws Exception {
        String RPC_QUEUE_NAME = "rpc_queue";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32773);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queueName = channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null).getQueue();
        channel.queuePurge(RPC_QUEUE_NAME);

        channel.basicQos(1);
        log.info(" [x] Awaiting RPC requests");

        Object monitor = new Object();

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();
            String response = "";

            try {
                String message = new String(delivery.getBody(), "UTF-8");
                int n = Integer.parseInt(message);

                log.info("[.] fib({})", message);
                response += fib(n);
            } catch (RuntimeException e) {
                log.error(" [.] {}" , e.toString());
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                synchronized (monitor) {
                    monitor.notify();
                }
            }
        });

        channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, consumerTag -> {});
        while (true) {
            synchronized (monitor) {
                try {
                    monitor.wait();
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }
        }
    }

}
