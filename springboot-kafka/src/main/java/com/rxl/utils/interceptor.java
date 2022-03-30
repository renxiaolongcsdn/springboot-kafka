package com.rxl.utils;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.util.ObjectUtils;

import java.util.Date;
import java.util.Map;

/**
 * @author ren.xiaolong
 * @date 2022/2/25
 * @Description  kafka 拦截器
 */
public class interceptor implements ProducerInterceptor{


    private Integer success=0;
    private Integer failed=0;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return new ProducerRecord(producerRecord.topic(),producerRecord.partition(),producerRecord.timestamp(),producerRecord.key(),new Date()+"-->"+producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(ObjectUtils.isEmpty(e)){
            success++;
        }else{
            failed++;
        }
    }

    @Override
    public void close() {
        System.out.println("success:"+success);
        System.out.println("failed:"+failed);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
