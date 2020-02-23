package com.liuencier.kafka.chapter3;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        System.out.println("before:" + consumerRecords);
        long currentTimeMillis = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        consumerRecords.partitions().forEach(topicPartition -> {
            List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            records.forEach(record -> {
                newTpRecords.add(record);
                if (currentTimeMillis - record.timestamp() < EXPIRE_INTERVAL) {

                }
            });
            if (!newTpRecords.isEmpty()) {
                newRecords.put(topicPartition, newTpRecords);
            }
        });
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        map.forEach(((topicPartition, offsetAndMetadata) -> {
            System.out.println("分区名:" + topicPartition + ",偏移量:" + offsetAndMetadata.offset() + ",元数据:" + offsetAndMetadata.metadata());
        }));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
