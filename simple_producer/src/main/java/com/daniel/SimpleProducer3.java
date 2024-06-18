package com.daniel;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer3 {
    public static void main(String[] args) {
        String server = "[::1]:9092";
        String topic = "MyFirstTopic1";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String key = "Key 2";
        String value = "Asynchronous producer message";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, new SimpleProducer3.ProducerCallback());
        producer.flush();
        producer.close();
    }

    static class ProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                System.out.println("Record delivery to broker failed: " + exception.getMessage());
                return;
            }
            System.out.printf("Message is sent to partition: %s. Offset: %s. Timestamp: %s", metadata.partition(),
                    metadata.offset(), metadata.timestamp());
        }

    }
}
