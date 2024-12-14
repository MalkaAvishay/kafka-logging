package com.example.kafkalogging.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @Value("${app.kafka.topic}")
    private String topic;

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "example-group")
    public void consume(String message) {
        System.out.println("Consumed message from topic " + topic + ": " + message);
    }
}

