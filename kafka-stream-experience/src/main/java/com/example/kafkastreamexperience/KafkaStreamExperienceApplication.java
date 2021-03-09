package com.example.kafkastreamexperience;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static com.example.kafkastreamexperience.StreamConsumer.consumeKafkaStreams;

@SpringBootApplication
public class KafkaStreamExperienceApplication {

    public static void main(String[] args) {

        SpringApplication.run(KafkaStreamExperienceApplication.class, args);

        consumeKafkaStreams();
    }

}
