package com.hank.rabbitmq.log;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by Hank on 6/20/2017.
 * It's a logging system
 */
public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // declare an exchange for producer
        /**
         * declare an exchange for producer
         * exchange type = fanout
         * broadcast to each queues
         */
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        String message = getMessage(args);

        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());

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
