package com.avro.generated.serializers;

import com.avro.generated.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class GenericOrderProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties)) {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse("{\n" +
                    "    \"namespace\": \"com.avro.generated\",\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"name\": \"TruckRecord\",\n" +
                    "    \"fields\": [\n" +
                    "        { \"name\": \"id\", \"type\": \"integer\" },\n" +
                    "        { \"name\": \"latitude\", \"type\": \"string\" },\n" +
                    "        { \"name\": \"longitude\", \"type\": \"string\" }\n" +
                    "    ]\n" +
                    "}");

            GenericRecord truckRecord = new GenericData.Record(schema);
            truckRecord.put("id", 1);
            truckRecord.put("latitude", "144,3 NE");
            truckRecord.put("longitude", "34 S");

            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("TruckRecordAvroGRTopic", truckRecord.get("id").toString(), truckRecord);
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
