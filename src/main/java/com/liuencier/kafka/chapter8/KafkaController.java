package com.liuencier.kafka.chapter8;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: kafka
 * @description: kafka控制器
 * @author: liuenci
 * @create: 2020-03-15 17:02
 **/
@Slf4j
@RestController
@RequestMapping("/kafka/req")
public class KafkaController {

    @RequestMapping("/index")
    public String index(){
        return "hello,kafka!";
    }

    @Autowired
    private KafkaTemplate kafkaTemplate;
    private static final String topic = "cier";

    @GetMapping("/send/{input}")
    public String sendToKafka(@PathVariable("input") String input){
//        kafkaTemplate.send(topic, input);
        kafkaTemplate.executeInTransaction(t -> {
            t.send(topic, input);
            if ("error".equals(input)) {
                throw new RuntimeException("input is error");
            }
            t.send(topic, input + " anthor");
            return true;
        });
        return "success";
    }

    @GetMapping("/send2/{input}")
    @Transactional(rollbackFor = Exception.class)
    public String send2ToKafka(@PathVariable("input") String input){
        kafkaTemplate.send(topic, input);
        if ("error".equals(input)) {
            throw new RuntimeException("input is error");
        }
        kafkaTemplate.send(topic, input + " anthor");
        return "success";
    }


    @KafkaListener(id = "", topics = topic, groupId = "group.cier")
    public void consumer(String input){
        log.info("input:{}", input);
    }
}
