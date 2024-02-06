package org.apache.kafkademo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoUsingKeys {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoUsingKeys.class.getSimpleName());

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

for (int j=0;j<2;j++) {

    for (int i = 0; i <= 10; i++) {

        String topic = "producer_demo_java";
        String key = "id_" + i;
        String value = "hello world " + i;

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic,key,value);

        //send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    log.info("Key " +key+" | Partition:  " + recordMetadata.partition());
                } else {
                    log.info(e.getMessage());
                }
            }
        });
    }

    try {
        Thread.sleep(500L);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }

}

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();



    }


}
