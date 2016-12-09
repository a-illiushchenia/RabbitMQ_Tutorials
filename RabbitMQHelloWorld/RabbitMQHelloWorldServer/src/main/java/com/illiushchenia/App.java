package com.illiushchenia;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class App
{
    private final String RABBIT_MQ_HOST = "localhost";
    private final String RABBIT_MQ_DEFAULT_EXCHANGE = "";
    private final String QUEUE_NAME = "hello";

    public static void main( String[] args ) throws IOException, TimeoutException {
        new App().sendMessageToRabbitMQ();
    }

    public void sendMessageToRabbitMQ() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_MQ_HOST);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello World!";
        channel.basicPublish(RABBIT_MQ_DEFAULT_EXCHANGE, QUEUE_NAME, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
