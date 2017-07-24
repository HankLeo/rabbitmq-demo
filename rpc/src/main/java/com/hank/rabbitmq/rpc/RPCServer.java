package com.hank.rabbitmq.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by Hank on 7/24/2017.
 */
public class RPCServer {

    private static final String requestQueueName = "rpc_queue";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n-1) + fib(n-2);
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare(requestQueueName, false, false, false, null);

        /**
         * In order to spread the load equally over multiple servers we need to set the  prefetchCount
         */
        channel.basicQos(1);

        System.out.println(" [x] Awaiting RPC requests");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                AMQP.BasicProperties replyPro = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(properties.getCorrelationId())
                        .build();

                String response = "";

                String message = new String(body,"UTF-8");
                int n = Integer.parseInt(message);
                System.out.println(" [.] fib(" + message + ")");
                response += fib(n);

                channel.basicPublish("", properties.getReplyTo(), replyPro, response.getBytes("UTF-8"));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(requestQueueName, false, consumer);

        /**
         *

        while (true) {
            try {
                Thread.sleep(100);
            }
            catch (Exception e) {}
        }
         */

        channel.close();
        if (connection != null) {
            try {
                connection.close();
            }
            catch (Exception e) {}
        }
    }

}
