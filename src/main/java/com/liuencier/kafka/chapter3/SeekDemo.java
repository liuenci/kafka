package com.liuencier.kafka.chapter3;

import com.liuencier.kafka.ConsumerClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
public class SeekDemo extends ConsumerClientConfig {

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        // timeout参数设置多少合适？太短会使分区分配失败，太长又有可能造成一些不必要的等待
        consumer.poll(Duration.ofMillis(2000));
        // 获取消费者所分配到的分区
        Set<TopicPartition> assignment = consumer.assignment();
        System.out.println(assignment);
        for (TopicPartition tp : assignment) {
            // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
            consumer.seek(tp, 100);
        }
//        consumer.seek(new TopicPartition(topic,0),10);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ":" + record.value());
            }
        }
    }

}
