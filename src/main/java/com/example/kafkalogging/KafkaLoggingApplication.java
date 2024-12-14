package com.example.kafkalogging;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaLoggingApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaLoggingApplication.class, args);
    }

}
