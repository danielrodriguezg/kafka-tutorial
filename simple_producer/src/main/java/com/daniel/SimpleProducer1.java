package com.daniel;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class SimpleProducer1 {

    public static void main(String[] args) {
        String server = "[::1]:9092";
        String topicName = "MyFirstTopic1";
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        String key = "Key1";
        String value = "Fire and Forget producer message";

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

        producer.send(record);
        producer.flush();
        producer.close();
        System.out.println("SimpleProducer Completed");
    }
}
