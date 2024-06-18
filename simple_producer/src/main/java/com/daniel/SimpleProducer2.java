package com.daniel;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer2 {
    public static void main(String[] args) {
        String server = "[::1]:9092";
        String topic = "MyFirstTopic1";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            String key = "Key 2";
            String value = "Synchronous producer message";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            RecordMetadata result = producer.send(record).get();

            System.out.printf("Message is sent to partition: %s. Offset: %s. Timestamp: %s", result.partition(),
                    result.offset(), result.timestamp());
        } catch (InterruptedException | ExecutionException e1) {
            System.out.printf("Erros sending record", e1);
        }

    }
}
