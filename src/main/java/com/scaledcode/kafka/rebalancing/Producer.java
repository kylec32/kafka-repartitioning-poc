package com.scaledcode.kafka.rebalancing;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
public class Producer {
    private final String id;
    private final int delay;

    public void run() {
        Properties producerProperties = ConfigurationHolder.producerConfiguration();
        int i = 0;
        while(true) {
            try(org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(producerProperties)) {
                String value = id + "-" + i++;

                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic",
                                                                                        value,
                                                                                        value);

                Future<RecordMetadata> sendResponse = producer.send(record);

                sendResponse.get(2, TimeUnit.SECONDS);
                System.out.println("Sent message: " + value);
                Thread.sleep(delay);
            } catch (InterruptedException | TimeoutException e) {
                System.out.println("Interrupt sending message");
                e.printStackTrace();
            } catch (ExecutionException e) {
                System.out.println("Issue sending message");
                e.printStackTrace();
            }
        }
    }
}
