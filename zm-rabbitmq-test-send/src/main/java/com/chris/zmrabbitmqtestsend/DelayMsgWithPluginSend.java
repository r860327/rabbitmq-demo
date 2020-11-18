package com.chris.zmrabbitmqtestsend;/*
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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by chris on 2020/11/10.
 * 使用死信队列实现延时消息
 * 发送端
 */
@Slf4j
@Component
public class DelayMsgWithPluginSend {
    private static String PLUGIN_QUEUE = "PLUGIN_DELAYED_QUEUE";
    private static String PLUGIN_DELAYED_EXCHANGE = "PLUGIN_DELAYED_EXCHANGE";

    private Connection connection;
    private Channel channel;

    public DelayMsgWithPluginSend() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32789);

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public void sendDelayMsg(String message, long delayTime) throws IOException {
        Map<String, Object> headers = new HashMap<String, Object>();
        // 通过header参数指定延时时间
        headers.put("x-delay", delayTime);

        AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                .headers(headers)
                .build();
        channel.basicPublish(PLUGIN_DELAYED_EXCHANGE, "DELAY_KEY", props, message.getBytes("UTF-8"));
        log.info("send message {} with delay {}ms", message, delayTime);
    }

    public void close() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }

}
