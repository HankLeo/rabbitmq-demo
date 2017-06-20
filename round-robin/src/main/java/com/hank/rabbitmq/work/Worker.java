package com.hank.rabbitmq.work;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by Hank on 6/16/2017.
 */
public class Worker {

    private final static String QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        /**
         * This tells RabbitMQ not to give more than one message to a worker at a time.
         * don't dispatch a new message to a worker until it has processed and acknowledged the previous one
         */
        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                String message = new String(body,"UTF-8");
                String currentTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()).toString();
                System.out.println(" [" + currentTime + "] Received '" + message + "'");

                try {
                    doWork(message);
                } finally {
                    System.out.println(" [" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()).toString()
                            + "] Done");
                    /**
                     * Message acknowledge after process done.
                     * Using this code we can be sure that even if you kill a worker using CTRL+C
                     * while it was processing a message, nothing will be lost.
                     * Soon after the worker dies all unacknowledged messages will be redelivered.
                     */
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        channel.basicConsume(QUEUE_NAME, false, consumer);
    }

    private static void doWork(String s) {
        for (char c : s.toCharArray()) {
            if (c == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
