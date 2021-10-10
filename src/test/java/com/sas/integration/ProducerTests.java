package com.sas.integration;


import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;

@Slf4j
public class ProducerTests extends BaseTest {

    public static final int MAX_INTERVAL_FOR_MESSAGE = 5;

    @Test
    public void producerShouldGenerateMessageRandomlyWithinInterval() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        final List<String> receivedMessages = collectMessages();
        //wait exactly 5 seconds
        await().pollDelay(MAX_INTERVAL_FOR_MESSAGE, TimeUnit.SECONDS).until(() -> true);

        //Producer generates message randomly. Interval is between 1 and 5 seconds
        log.info("Received messages: {}", receivedMessages);
        int numberOfMessages = receivedMessages.size();
        Assert.assertTrue(numberOfMessages >= 1, "Received less messages than expected! Number of messages - " + numberOfMessages);
        Assert.assertTrue(numberOfMessages <= 5, "Received more messages than expected! Number of messages - " + numberOfMessages);
    }

    @Test
    public void producerShouldGenerateLowerCaseMessages() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        final List<String> receivedMessages = collectMessages();
        //wait for first message
        await().atMost(MAX_INTERVAL_FOR_MESSAGE, TimeUnit.SECONDS).until(() -> !receivedMessages.isEmpty());

        log.info("Received messages: {}", receivedMessages);
        Assert.assertTrue(StringUtils.isAllLowerCase(receivedMessages.get(0)), "Message is not in lower case!");
    }

    private List<String> collectMessages() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        Channel channel = getChannel();
        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, SPRING_BOOT_EXCHANGE, "sas.#");
        final List<String> receivedMessages = new ArrayList<String>();

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                receivedMessages.add(message);
            }
        };

        channel.basicConsume(queue, consumer);
        return receivedMessages;
    }

}
