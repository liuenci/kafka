package com.liuencier.kafka.chapter2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerAnalysis {
    private static final String brokerList = "localhost:9092";
    private static final String topic = "cier";

    public static Properties initConfig() {
        Properties props = new Properties();
        // 该属性指定 brokers 的地址清单，格式为 host:port。清单里不需要包含所有的 broker 地址，
        // 生产者会从给定的 broker 里查找到其它 broker 的信息。——建议至少提供两个 broker 的信息，因为一旦其中一个宕机，生产者仍然能够连接到集群上。
        props.put("bootstrap.servers", brokerList);
        // 将 key 转换为字节数组的配置，必须设定为一个实现了 org.apache.kafka.common.serialization.Serializer 接口的类，
        // 生产者会用这个类把键对象序列化为字节数组。
        // ——kafka 默认提供了 StringSerializer和 IntegerSerializer、ByteArraySerializer。当然也可以自定义序列化器。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 和 key.serializer 一样，用于 value 的序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 用来设定KafkaProducer对应的客户端ID，默认为空，如果不设置KafkaProducer会自动生成一个非空字符串。
        // 内容形式如："producer-1"
        props.put("client.id", "producer.client.id.demo");
        return props;
    }

    public static Properties initNewConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");

        // 自定义分区器的使用
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,DefinePartitioner.class.getName());

        // 自定义拦截器使用
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        return props;
    }

    public static Properties initPerferConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return props;
    }

    public static void main(String[] args) throws InterruptedException {
        Properties props = initNewConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

//        KafkaProducer<String, String> producer = new KafkaProducer<>(props,
//                new StringSerializer(), new StringSerializer());
        //生成 ProducerRecord 对象，并制定 Topic，key 以及 value
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Kafka-demo-001", "卢本伟牛逼！！！！！");
        try {
            // 1、发送消息
            producer.send(record);

            // 2、同步发送
            //通过send()发送完消息后返回一个Future对象,然后调用Future对象的get()方法等待kafka响应
            //如果kafka正常响应，返回一个RecordMetadata对象，该对象存储消息的偏移量
            // 如果kafka发生错误，无法正常响应，就会抛出异常，我们便可以进行异常处理
            //producer.send(record).get();

            // 3、异步发送
//            producer.send(record, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    if (exception == null) {
//                        System.out.println(metadata.partition() + ":" + metadata.offset());
//                    }
//                }
//            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
//        TimeUnit.SECONDS.sleep(5);
    }
}
