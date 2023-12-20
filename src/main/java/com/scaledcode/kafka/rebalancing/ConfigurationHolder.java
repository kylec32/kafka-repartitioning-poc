package com.scaledcode.kafka.rebalancing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ConfigurationHolder {
    public static Properties commonKafkaConfiguration() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka.local.com:9092");
        properties.put("basic.auth.credentials.source", "USER_INFO");
        properties.put("schema.registry.basic.auth.user.info", "pw");
        properties.put("sasl.jaas.config", "");
        properties.put("sasl.mechanism", "");
        properties.put("security.protocol", "PLAINTEXT");
        return properties;
    }

    public static Properties producerConfiguration() {
        Properties properties = commonKafkaConfiguration();
        properties.put("acks", "all");
        properties.put("compression.type", "snappy");
        properties.put("retries", "3");
        properties.put("ssl.endpoint.identification.algorithm", "");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        return properties;
    }

    public static Properties consumerProperties() {
        Properties properties = commonKafkaConfiguration();
        properties.put("auto.commit.interval.ms", "3000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "true");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        properties.put("ssl.endpoint.identification.algorithm", "");
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, WeightedPartitionAssignor.class.getName());

        return properties;
    }
}
