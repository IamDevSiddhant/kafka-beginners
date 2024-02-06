package io.kafka.consumer.demo;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient(){
    String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
        // REST client without security
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
        // REST client with security
        String[] auth = userInfo.split(":");

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


    }

        return restHighLevelClient;
}


public static KafkaConsumer<String ,String> consumer(){
    String groupId = "consumer-opensearch";
    String localHost="127.0.0.1:9092";
    Properties properties=new Properties();
    //connect to localhost
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,localHost);
    //set consumer config props
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    return new KafkaConsumer<>(properties);
}


public static String extractId(String json){

        return JsonParser.parseString(json)
                .getAsJsonObject()
                .getAsJsonObject("meta")
                .get("id")
                .getAsString();

}
    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //first create opensearch client

        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create kafka consumer
        KafkaConsumer<String,String> kafkaConsumer=consumer();
        //we need to create index on opensearch if it doesn't exist already


        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                kafkaConsumer.wakeup(); //this is explicitly throw exception if shutdown is occured

                // join the main thread to allow the execution of the code in the main thread

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        //when we use opensearchClient in try block as argument
        //regardless of exception occurs or not opensearchClient will be closed!
        try(openSearchClient;kafkaConsumer){
            boolean isIndexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!isIndexExist){
                CreateIndexRequest createIndexRequest=new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest,RequestOptions.DEFAULT);
                log.info("New Index Created on Open search ");
            }
            else {
                log.info("Index already exists");
            }

            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true){
                ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(3));
                int recordCount = consumerRecords.count();
                log.info("received "+recordCount+" record(s)");

                BulkRequest bulkRequest=new BulkRequest();

                for (ConsumerRecord<String,String>r:consumerRecords){
                    try{

                        String id = extractId(r.value());

                        IndexRequest indexRequest=new IndexRequest("wikimedia")
                                .source(r.value(), XContentType.JSON).id(id);

                        //IndexResponse indexResponse=openSearchClient.index(indexRequest,RequestOptions.DEFAULT);
                        bulkRequest.add(indexRequest);
                        //log.info(indexResponse.getId());

                    }catch (Exception e){
                        log.info(e.getMessage());
                    }
                }

                if (bulkRequest.numberOfActions()>0){
                    BulkResponse bulkResponse=openSearchClient.bulk(bulkRequest,RequestOptions.DEFAULT);
                    log.info("Inserted "+bulkResponse.getItems().length+" records");

                }


            }




        }catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        }catch (Exception e){
            log.info("Unexpected exception in the consumer", e);
        }finally {
            kafkaConsumer.close();
            log.info("The consumer is now gracefully shut down");
        }



    }
}
