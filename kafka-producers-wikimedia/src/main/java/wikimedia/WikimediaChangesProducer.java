package wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties properties=new Properties();
        //connect to localhost
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //set producer props
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create producer

        KafkaProducer<String,String> producer=new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler=new WikimediaChangeHandler(producer,topic);

        String url="https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder=new EventSource.Builder(eventHandler, URI.create(url));

        EventSource eventSource= builder.build();

        eventSource.start();

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);



    }
}
