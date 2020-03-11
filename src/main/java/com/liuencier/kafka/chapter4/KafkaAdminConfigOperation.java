package com.liuencier.kafka.chapter4;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @program: kafka
 * @description: KafkaAdminClient应用
 * @author: liuenci
 * @create: 2020-03-10 18:28
 **/
public class KafkaAdminConfigOperation {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        describeTopicConfig();
//        alterTopicConfig();
        addTopicPartitions();
    }

    public static void describeTopicConfig() throws ExecutionException, InterruptedException {
        String brokerList = "localhost:9092";
        String topic = "cier";
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        Config config = result.all().get().get(resource);
        System.out.println("config:" + config);
        client.close();
    }

    public static void alterTopicConfig() throws ExecutionException, InterruptedException {
        String brokerList = "localhost:9092";
        String topic = "cier";
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
        Config config = new Config(Collections.singleton(entry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(resource, config);
        AlterConfigsResult result = client.alterConfigs(configs);
        result.all().get();
        client.close();
    }

    public static void addTopicPartitions() throws ExecutionException, InterruptedException {
        String brokerList = "localhost:9092";
        String topic = "cier";
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic, newPartitions);
        CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        result.all().get();
        client.close();
    }
}
