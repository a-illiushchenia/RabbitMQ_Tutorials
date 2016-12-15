package com.illiushchenia;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class App
{
    private final String RABBIT_MQ_HOST = "localhost";
    private final String RABBIT_MQ_EXCHANGE_NAME = "logs";
    private final String RABBIT_MQ_EXCHANGE_TYPE = "fanout";
    private final Integer THREAD_SLEEP_DELAY = 1000;

    // выключаю подтверждение обработки сообщений
    private final boolean AUTO_ACK = true;

    public static void main( String[] args ) throws IOException, TimeoutException {
        new App().receiveMessage();
    }

    public void receiveMessage() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_MQ_HOST);
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();


        //создаю Exchange
        channel.exchangeDeclare(RABBIT_MQ_EXCHANGE_NAME, RABBIT_MQ_EXCHANGE_TYPE);
        //создаю временную очередь
        String queueName = channel.queueDeclare().getQueue();
        //связываю Exchange с временно очередью
        channel.queueBind(queueName, RABBIT_MQ_EXCHANGE_NAME, "");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException{
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };

        channel.basicConsume(queueName, AUTO_ACK, consumer);
    }
}
