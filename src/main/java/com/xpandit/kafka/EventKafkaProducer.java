package com.xpandit.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Created by xpandit on 7/29/16.
 */
public class EventKafkaProducer {

    private static final String BROKERS = "localhost:9092";
    private static final String TOPIC = "events";

    private static final int INPUT_SIZE = 200000;           //number of events being produced to kafka each INTERVAL_TIME_MS
    private static final int INTERVAL_TIME_MS = 1000;


    public static void main(String[] args){


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        LineIterator it = null;

        try {

            it = FileUtils.lineIterator(new File("src/main/resources/input/events.txt"), "UTF-8");

            int currentEvent = 0;
            int nextStop = INPUT_SIZE;

            while(it.hasNext()) {

                while(currentEvent < nextStop) {

                    String line = it.nextLine();
                    String msg = System.currentTimeMillis() + "|" + line;

                    ProducerRecord<String,String> producerRecord = new ProducerRecord<>(TOPIC, null/*key*/, msg);
                    producer.send(producerRecord);          //async send
                    currentEvent++;
                }

                System.out.println("Injected " + INPUT_SIZE + " events to Kafka  [Total: " + currentEvent + "]");

                nextStop += INPUT_SIZE;

                //Thread.sleep(INTERVAL_TIME_MS);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            producer.close();
            LineIterator.closeQuietly(it);
        }
    }
}
