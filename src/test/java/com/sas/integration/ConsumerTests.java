package com.sas.integration;


import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ConsumerTests extends BaseTest {

    @DataProvider(name = "messages")
    public Object[][] messages(){
        return new Object[][]{
                {"lowcase"},
                {"UPPERCASE"},
                {"mixedCASE"}
        };
    }

    @Test(dataProvider = "messages")
    public void consumerShouldReturnMessageInUpperCase(String message) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, InterruptedException, TimeoutException {
        Channel channel = getChannel();
        final BlockingQueue<String> response = sendMessageAndGetResponse(channel, message);

        String responseMessage = response.take();
        log.info("Received response {}: ", responseMessage);
        Assert.assertTrue(StringUtils.isAllUpperCase(responseMessage));
    }
}
