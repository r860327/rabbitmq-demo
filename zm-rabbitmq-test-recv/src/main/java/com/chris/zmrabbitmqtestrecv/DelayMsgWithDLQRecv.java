package com.chris.zmrabbitmqtestrecv;/*
 * Copyright 2020 zhimatech.
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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by chris on 2020/11/10.
 * 使用死信队列实现延时消息
 * 接收端
 */
@Slf4j
@Component
public class DelayMsgWithDLQRecv {
    private static String ORIGIN_QUEUE = "TEST_ORIGIN_QUEUE";
    private static String DXL_EXCHANGE = "DXL_EXCHANGE";
    private static String DXL_QUEUE = "DXL_QUEUE";

    private Connection connection;
    private Channel channel;

    public DelayMsgWithDLQRecv() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32789);

        connection = factory.newConnection();
        channel = connection.createChannel();

        Map<String, Object> arguments = new HashMap<>();
        // 指定原队列中消息过期后对应的死信exchange
        arguments.put("x-dead-letter-exchange", DXL_EXCHANGE);
        // 声明消息投递原队列
        channel.queueDeclare(ORIGIN_QUEUE, false, false, false, arguments);

        // 声明死信交换机
        channel.exchangeDeclare(DXL_EXCHANGE, "topic", false, false, false, null);
        // 声明死信队列
        channel.queueDeclare(DXL_QUEUE, false, false, false, null);
        // 绑定死信队列
        channel.queueBind(DXL_QUEUE, DXL_EXCHANGE, "#");
    }

    public void recv() throws IOException {
        log.info(" [*] Waiting for message from delay queue. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            log.info(" [x] Received '{}'", message);
        });

        channel.basicConsume(DXL_QUEUE, true, deliverCallback, consumerTag -> {});
    }
}
