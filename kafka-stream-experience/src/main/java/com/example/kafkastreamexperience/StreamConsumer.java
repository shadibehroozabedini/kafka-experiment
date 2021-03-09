package com.example.kafkastreamexperience;

import com.example.kafkastreamexperience.common.AppConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class StreamConsumer {

    private static final Logger LOGGER = LogManager.getLogger(StreamConsumer.class);

    static void consumeKafkaStreams() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> kStream = builder.stream(AppConfigs.TOPIC_NAME);
        kStream.foreach((k, v) -> System.out.println("Key = " + k + " Value = " + v));
        //kStream.peek((k, v) -> System.out.println("Key = " + k + " Value = " + v));
        Topology topology = builder.build();

       KafkaStreams streams = new KafkaStreams(topology, initializeProperties());
            LOGGER.info("Starting the stream");
            streams.start();
            LOGGER.info("Stopping Stream");


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Stopping Stream");
            streams.close();
        }));
    }

    private static Properties initializeProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
