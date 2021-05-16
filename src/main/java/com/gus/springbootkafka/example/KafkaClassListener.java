package com.gus.springbootkafka.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(id = "class-level", topics = "reflectoring-3")
class KafkaClassListener {

    Logger LOG = LoggerFactory.getLogger(KafkaClassListener.class);

    /* Using @KafkaListener At Class Level */

    @KafkaHandler
    void listen(String message) {
        LOG.info("KafkaHandler[String] {}", message);
    }

    @KafkaHandler(isDefault = true)
    void listenDefault(Object object) {
        LOG.info("KafkaHandler[Default] {}", object);
    }
    /* Since we have specified initialOffset = "0", we will receive all the messages starting from offset 0 every time we restart the application. */
    @KafkaListener(groupId = "reflectoring-group-3",
                    topicPartitions = @TopicPartition(
                    topic = "reflectoring-1",
                    partitionOffsets = { @PartitionOffset(
                            partition = "0",
                            initialOffset = "0") }))
    void listenToPartitionWithOffset(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset) {
        LOG.info("Received message [{}] from partition-{} with offset-{}",
                message,
                partition,
                offset);
    }

    @KafkaListener(topics = "reflectoring-others")
    @SendTo("reflectoring-1")
    String listenAndReply(String message) {
        LOG.info("ListenAndReply [{}]", message);
        return "This is a reply sent after receiving message";
    }
}