package com.example.consumer;

import com.example.consumer.common.AppConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

import static com.example.consumer.common.AppConfigs.TOPIC_NAME;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class Consumer {
    static void consumeMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,AppConfigs.APPLICATION_ID);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.BOOTSTRAP_SERVERS);
        props.put(GROUP_ID_CONFIG, AppConfigs.GROUP_ID_CONFIG);
        //props.put("enable.auto.commit", "true");
        //props.put("auto.commit.interval.ms", "1000");
        //props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

   /* props.put("key.deserializer",
            "org.apache.kafka.common.serializa-tion.StringDeserializer");
    props.put("value.deserializer",
            "org.apache.kafka.common.serializa-tion.StringDeserializer");*/
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer
                <Integer, String>(props);
        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        //print the topic name
        System.out.println("Subscribed to topic " + TOPIC_NAME);
        int i = 0;


        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer, String> record : records)

                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %d, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }

}
