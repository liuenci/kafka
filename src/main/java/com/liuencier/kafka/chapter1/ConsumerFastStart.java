package com.liuencier.kafka.chapter1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
@Slf4j
public class ConsumerFastStart {
    private static final String brokerList = "localhost:9092";

    private static final String topic = "cier";

    private static final String groupId = "ccier";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置序列化器
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置值序列化器
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置集群地址
//        properties.put("bootstrap.servers", brokerList);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // 设置消费组
//        properties.put("group.id", groupId);

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("消费信息:{}", record.value());
                consumer.commitSync();
            }
        }
    }
}
