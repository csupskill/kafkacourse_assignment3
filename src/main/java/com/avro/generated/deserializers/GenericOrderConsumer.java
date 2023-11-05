package com.avro.generated.deserializers;

import com.avro.generated.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GenericOrderConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty("specific.avro.reader", "true");
        properties.setProperty("group.id", "AvroOrderGroup");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("TruckRecordAvroGRTopic"));

        ConsumerRecords<String, GenericRecord> orderConsumerRecords = consumer.poll(Duration.ofMillis(750));

        for (ConsumerRecord<String, GenericRecord> record : orderConsumerRecords) {
            System.out.println("The truck with id " + record.value().get("id").toString() + " has arrived.");
        }

        consumer.close();
    }
}
