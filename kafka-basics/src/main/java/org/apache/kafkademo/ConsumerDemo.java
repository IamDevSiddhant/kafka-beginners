package org.apache.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "producer_demo_java";

        //create consumer props
        Properties properties=new Properties();
        //connect to localhost
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //set consumer config props
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data

        while (true){
            log.info("polling");

            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofSeconds(3L));

            for (ConsumerRecord<String,String> consumerRecords:records){
                log.info("key: "+consumerRecords.key()+" ,value: "+consumerRecords.value());
                log.info("partitions: "+consumerRecords.partition()+" ,offset "+consumerRecords.offset());
            }
        }




    }


}
