package wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class WikimediaChangeHandler implements EventHandler {

    public final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());


    String topic;

    KafkaProducer<String,String>kafkaProducer;

    public WikimediaChangeHandler(KafkaProducer<String,String>kafkaProducer,String topic){
        this.kafkaProducer=kafkaProducer;
        this.topic=topic;
    }

    @Override
    public void onOpen()  {
        //Nothing Here
    }

    @Override
    public void onClosed()  {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        //asynchronous
        kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
