package com.example.producer;

import com.example.producer.common.AppConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.springframework.kafka.listener.ConsumerAwareRebalanceListener.LOGGER;

public class Producer {

    static void produceMessages() {
        LOGGER.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.APPLICATION_ID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*I use try with resources then it will be automatically closed
        in real life application producer should never been closed*/
        try (
                org.apache.kafka.clients.producer.Producer<Integer, String> producer = new KafkaProducer<>(props);) {

            LOGGER.info("Start sending messages...");
            for (int i = 1; i <= AppConfigs.NUMBER_EVENTS; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.TOPIC_NAME, i, "Simple Message-" + i));
            }

            LOGGER.info("Finished - Closing Kafka Producer.");
        }
    }

       /*
       Original code
       KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

       LOGGER.info("Start sending messages...");
        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-" + i));
        }

        LOGGER.info("Finished - Closing Kafka Producer.");
        producer.close();*/ // in real life application producer should never been closed

}
