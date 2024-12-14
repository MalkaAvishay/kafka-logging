package com.example.kafkalogging.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;

import java.util.Map;

import static com.example.kafkalogging.util.HeaderUtil.convertToSerializableMap;
import static net.logstash.logback.argument.StructuredArguments.kv;

public class ProducerLoggingInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ProducerLoggingInterceptor.class);

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
        log.info("OnProduce",
                kv("topic", producerRecord.topic()),
                kv("key", producerRecord.key()),
                kv("partition", producerRecord.partition()),
                kv("timestamp", producerRecord.timestamp()),
                kv("headers", convertToSerializableMap(producerRecord.headers())),
                kv("payload", producerRecord.value())
        );
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }

}
