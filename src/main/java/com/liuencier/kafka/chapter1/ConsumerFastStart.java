package com.liuencier.kafka.chapter1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
@Slf4j
public class ConsumerFastStart {
    private static final String brokerList = "localhost:9092";

    private static final String topic = "heima";

    private static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置序列化器
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置值序列化器
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置集群地址
        properties.put("bootstrap.servers", brokerList);
        // 设置消费组
        properties.put("group.id", groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info(record.value());
            }
        }
    }
}
