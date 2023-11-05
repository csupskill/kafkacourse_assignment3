package com.avro.generated.serializers;

import com.avro.generated.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(properties)) {
            Order order = new Order("John", "The Phone", 3);
            ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic", order.getCustomerName().toString(), order);
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
