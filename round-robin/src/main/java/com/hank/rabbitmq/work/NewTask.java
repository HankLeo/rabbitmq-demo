package com.hank.rabbitmq.work;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Round-robin dispatch
 * One task is dispatched to many workers in in sequence.
 * Created by Hank on 6/16/2017.
 */
public class NewTask {

    private final static String QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /**
         * Durable param:
         * set it true will never lose our queue even if rabbitmq server down
         * RabbitMQ doesn't allow to redefine an existing queue with different parameters
         */
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        String message = getMessage(args);
        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
        String currentTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()).toString();
        System.out.println(" [" + currentTime + "] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 1) {
            return "Hello RabbitMQ!";
        }
        return joinStrings(strings, " ");
    }

    private static String joinStrings(String[] strings, String delimiter) {
        int len = strings.length;
        if (len == 0) {
            return "";
        }
        StringBuffer stringBuffer = new StringBuffer(strings[0]);
        for (int i = 1; i < len; i++) {
            stringBuffer.append(delimiter).append(strings[i]);
        }
        return stringBuffer.toString();
    }

}
