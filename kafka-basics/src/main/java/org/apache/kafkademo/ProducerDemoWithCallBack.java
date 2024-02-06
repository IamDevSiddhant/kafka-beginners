package org.apache.kafkademo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am kafka producer");

        //create producer props
        Properties properties=new Properties();
        //connect to localhost
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //set producer props
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //below prop is not recommended just to demo StickyPartition we are setting below prop
        properties.setProperty("batch.size", "400");

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

for (int j=0;j<10;j++) {

    for (int i = 0; i <= 10; i++) {

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("producer_demo_java", "I love kafka " + i);

        //send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    log.info("Received new metadata \n" +
                            "Topic:  " + recordMetadata.topic() + "\n" +
                            "Partition:  " + recordMetadata.partition() + "\n" +
                            "Timestamp:  " + recordMetadata.timestamp());
                } else {
                    log.info(e.getMessage());
                }
            }
        });
    }
}

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();



    }


}
