package com.rxl.controller;

import com.rxl.kafka.KafkaUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ren.xiaolong
 * @date 2022/1/18
 * @Description
 */
@RestController
public class KafkaController {


    @Autowired
    KafkaUtil<String> kafkaUtil;

    @GetMapping("/demo")
    public void demo(){
        for (int i = 0; i < 10; i++) {
            kafkaUtil.send(i+"","test");
        }
    }
}
