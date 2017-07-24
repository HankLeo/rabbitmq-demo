package com.hank.rabbitmq.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * Created by Hank on 7/24/2017.
 */
public class RPCClient {

    private Connection connection;
    private Channel channel;
    private static final String requestQueueName = "rpc_queue";
    private String replyQueueName;

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        /**
         * When the Client starts up, it creates an anonymous exclusive callback queue.
         */
        replyQueueName = channel.queueDeclare().getQueue();
    }

    public String call(String message) throws IOException, InterruptedException {
        /**
         * correlationId is set to a unique value for every request
         * replyTo is set to the callback queue
         */
        final String corrId = UUID.randomUUID().toString();
        AMQP.BasicProperties properties = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, properties, message.getBytes("UTF-8"));

        /**
         * Subscribe to the 'callback' queue, so that we can receive RPC responses
         * Consumer delivery handling is happening in a separate thread,
         * so use BlockingQueue to suspend main thread before response arrives
         * Capacity set to 1 as we need to wait for only one response
         */
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                if (properties.getCorrelationId().equals(corrId)) {
                    response.offer(new String(body, "UTF-8"));
                }
            }
        };

        return response.take();
    }

    public void close() throws IOException {
        connection.close();
    }

    public static void main(String[] args) {
        RPCClient fibonacciRpc = null;
        String response = null;
        try {
            fibonacciRpc = new RPCClient();
            System.out.println(" [x] Requesting fib(30)");
            response = fibonacciRpc.call("30");
            System.out.println(" [.] Got '" + response + "'");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                fibonacciRpc.close();
            }
            catch (Exception e) {}
        }
    }

}
