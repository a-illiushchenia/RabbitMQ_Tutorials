package com.illiushchenia;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class App
{
    private final String RABBIT_MQ_HOST = "localhost";
    private final String QUEUE_NAME = "hello";
    private final Integer THREAD_SLEEP_DELAY = 1000;

    //включаю устойчивость очереди, т.е. при падении RabbitMQ она появиться после того, как RabbitMQ подниметься
    private final boolean DURABLE = true;
    // включаю подтверждение обработки сообщений
    private final boolean AUTO_ACK = false;
    //данный параметр гарантирует то, что подписчик не получит новое сообщение, до тех пор пока не обработает и не подтвердит предыдущее
    private final int PREFETCH_COUNT = 1;

    public static void main( String[] args ) throws IOException, TimeoutException {
        new App().receiveMessage();
    }

    public void receiveMessage() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_MQ_HOST);
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();


        channel.queueDeclare(QUEUE_NAME, DURABLE, false, false, null);
        channel.basicQos(PREFETCH_COUNT);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException{
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                try {
                    doWork(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println(" [x] Done");
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }

            private void doWork(String message) throws InterruptedException {
                for (char ch : message.toCharArray()){
                    if(ch == '.') {
                        Thread.sleep(THREAD_SLEEP_DELAY);
                    }
                }
            }
        };



        channel.basicConsume(QUEUE_NAME, AUTO_ACK, consumer);
    }
}
