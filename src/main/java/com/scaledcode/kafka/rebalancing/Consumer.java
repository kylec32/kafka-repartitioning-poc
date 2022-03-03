package com.scaledcode.kafka.rebalancing;

import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    @Getter
    private static String id = "unknown";
    private final String groupId;
    private final int delay;

    public Consumer(String id, String groupId, int delay) {
        this.id = id;
        this.groupId = groupId;
        this.delay = delay;
    }

    @SneakyThrows
    public void run() {
        final Properties properties = new Properties();
        properties.putAll(ConfigurationHolder.consumerProperties());
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("test-topic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("Revoking the following partitions: " );
                collection.forEach(System.out::println);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("Assigning the following partitions: " );
                collection.forEach(System.out::println);
            }
        });

        while(true) {
            var records = consumer.poll(Duration.ofSeconds(2));

            records.forEach(record -> {
                System.out.println("Consumer " + id + " received the record: " + record.key() + ", " + record.value());
            });

            Thread.sleep(delay);
        }
    }
}
