package com.illiushchenia;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class App
{
    private final String RABBIT_MQ_HOST = "localhost";
    private final String RABBIT_MQ_DEFAULT_EXCHANGE = "";
    private final String QUEUE_NAME = "hello";

    //включаю устойчивость очереди, т.е. при падении RabbitMQ она появиться после того, как RabbitMQ подниметься
    private final boolean DURABLE = true;
    //включаю устойчивость сообщения, т.е. при падении RabbitMQ оно появиться после того, как RabbitMQ подниметься
    private final AMQP.BasicProperties DURABLE_MESSAGE = MessageProperties.PERSISTENT_TEXT_PLAIN;

    public static void main( String[] args ) throws IOException, TimeoutException {
        new App().sendMessageToRabbitMQ(args);
    }

    public void sendMessageToRabbitMQ(String[] strings) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_MQ_HOST);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, DURABLE, false, false, null);
        String message = getMessage(strings);
        channel.basicPublish(RABBIT_MQ_DEFAULT_EXCHANGE, QUEUE_NAME, DURABLE_MESSAGE, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    private String getMessage(String[] strings) {
        if(strings.length < 1){
            return "Hello World!";
        }
        return joinStrings(strings, " ");
    }

    private String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuffer words = new StringBuffer(strings[0]);
        for (String s1 : strings){
             words.append(delimiter).append(s1);
        }
        return words.toString();
    }
}
