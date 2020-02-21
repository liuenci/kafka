package com.liuencier.kafka.chapter1;

import com.google.gson.internal.$Gson$Preconditions;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerFastStart {

    private static final String brokerList = "localhost:9092";

    private static final String topic = "cier";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置序列化器
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        // 设置值序列化器
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置集群地址
//        properties.put("bootstrap.servers", brokerList);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "kafka", "hello,cier");
//        Future<RecordMetadata> metadataFuture = producer.send(record);
        try {
//            RecordMetadata recordMetadata = metadataFuture.get();
//            System.out.println("topic:"+recordMetadata.topic());
//            System.out.println("partition:"+recordMetadata.partition());
//            System.out.println("offset:"+recordMetadata.offset());

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("topic:"+recordMetadata.topic());
                        System.out.println("partition:"+recordMetadata.partition());
                        System.out.println("offset:"+recordMetadata.offset());
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
