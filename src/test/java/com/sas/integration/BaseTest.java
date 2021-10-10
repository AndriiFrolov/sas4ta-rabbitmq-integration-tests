package com.sas.integration;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class BaseTest {


    public static final String QUEUE = "spring-boot";
    public static final String SPRING_BOOT_EXCHANGE = "spring-boot-exchange";
    public static final String URI = "amqps://inyuqldo:6HyvxRtVvofCOChYEoXB62TGsc2Z20jm@cattle.rmq2.cloudamqp.com/inyuqldo";
    public static final String HOST = "cattle.rmq2.cloudamqp.com";


    protected Channel getChannel() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        Connection connection = getConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(SPRING_BOOT_EXCHANGE, BuiltinExchangeType.TOPIC, true);
        return channel;
    }

    protected BlockingQueue<String> sendMessageAndGetResponse(final Channel channel, String message) throws IOException {
        String replyQueue = channel.queueDeclare().getQueue();
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder().replyTo(replyQueue).build();
        channel.basicPublish("", QUEUE, basicProperties, message.getBytes());
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        channel.basicConsume(replyQueue, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws UnsupportedEncodingException, UnsupportedEncodingException {
                response.offer(new String(body, StandardCharsets.UTF_8));
            }
        });
        return response;
    }

    private Connection getConnection() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI);
        factory.setHost(HOST);

        return factory.newConnection();
    }
}
