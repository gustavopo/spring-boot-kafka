package com.gus.springbootkafka.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListenerExample {

    Logger LOG = LoggerFactory.getLogger(KafkaListenerExample.class);

    /* Using @KafkaListener At Method Level */

    @KafkaListener(topics = "reflectoring-1")
    void listener(String data) {
        LOG.info(data);
    }

    @KafkaListener(
            topics = "reflectoring-1, reflectoring-2",
            groupId = "reflectoring-group-2")
    void commonListenerForMultipleTopics(String message) {
        LOG.info("MultipleTopicListener - {}", message);
    }

    @KafkaListener(
            topics = "reflectoring-user",
            groupId="reflectoring-user",
            containerFactory="userKafkaListenerContainerFactory")
    void listener(User user) {
        LOG.info("CustomUserListener [{}]", user);
    }
}


