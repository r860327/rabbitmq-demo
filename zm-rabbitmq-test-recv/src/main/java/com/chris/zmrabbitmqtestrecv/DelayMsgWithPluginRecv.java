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
 * Created by chris on 2020/11/13.
 */
@Slf4j
@Component
public class DelayMsgWithPluginRecv {
    private static String PLUGIN_QUEUE = "PLUGIN_DELAYED_QUEUE";

    private static String PLUGIN_DELAYED_EXCHANGE = "PLUGIN_DELAYED_EXCHANGE";

    private Connection connection;
    private Channel channel;

    public DelayMsgWithPluginRecv() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32789);

        connection = factory.newConnection();
        channel = connection.createChannel();

        Map<String, Object> arguments = new HashMap<>();
        /**
         * 延时插件支持的x-delayed-message 类型的exchange有点类似一个exchange proxy，只是通过这个exchange来判断消息是否需要延时
         * 以及延时的时间，具体消息的分发还是要通过原生的exchange支持，通过在创建exchange的参数中指定x-delayed-type来指定原生的
         * exchange类型
         */
        arguments.put("x-delayed-type", "direct");

        // 声明交换机
        channel.exchangeDeclare(PLUGIN_DELAYED_EXCHANGE, "x-delayed-message", false, false, false, arguments);
        // 声明队列
        channel.queueDeclare(PLUGIN_QUEUE, false, false, false, null);
        // 绑定交换机与队列 由于是direct 的exchange 可以通过指定routingKey来实现消息分发
        channel.queueBind(PLUGIN_QUEUE, PLUGIN_DELAYED_EXCHANGE, "DELAY_KEY");

    }

    public void recv() throws IOException {
        log.info(" [*] Waiting for message from delay queue. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            log.info(" [x] Received '{}'", message);
        });

        channel.basicConsume(PLUGIN_QUEUE, true, deliverCallback, consumerTag -> {});
    }

}
