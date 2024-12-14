package com.example.kafkalogging.config;

import com.example.kafkalogging.interceptor.ConsumerLoggingInterceptor;
import com.example.kafkalogging.interceptor.ProducerLoggingInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public DefaultKafkaProducerFactoryCustomizer defaultKafkaProducerFactoryCustomizer() {
        return producerFactory -> producerFactory.updateConfigs(Map.of(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerLoggingInterceptor.class.getName()));
    }

    @Bean
    public DefaultKafkaConsumerFactoryCustomizer defaultKafkaConsumerFactoryCustomizer() {
        return consumerFactory -> consumerFactory.updateConfigs(Map.of(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerLoggingInterceptor.class.getName()));
    }

}
