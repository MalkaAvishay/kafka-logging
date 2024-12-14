package com.example.kafkalogging.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Map;

import static com.example.kafkalogging.util.HeaderUtil.convertToSerializableMap;
import static net.logstash.logback.argument.StructuredArguments.kv;

public class ConsumerLoggingInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ConsumerLoggingInterceptor.class);

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> consumerRecords) {
        consumerRecords.forEach(record -> log.info("OnConsume",
                kv("topic", record.topic()),
                kv("key", record.key()),
                kv("partition", record.partition()),
                kv("offset", record.offset()),
                kv("timestamp", record.timestamp()),
                kv("keySize", record.serializedKeySize()),
                kv("valueSize", record.serializedValueSize()),
                kv("headers", convertToSerializableMap(record.headers())),
                kv("payload", record.value())
        ));
        return consumerRecords;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
