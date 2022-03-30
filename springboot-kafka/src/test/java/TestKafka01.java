import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.springframework.util.ObjectUtils;

import java.util.*;

/**
 * @author ren.xiaolong
 * @date 2022/2/25
 * @Description
 */
public class TestKafka01 {


    //不带回调函数的 API
    @Test
    public void test01(){
        Properties properties=new Properties();
        properties.put("bootstrap.servers","192.168.226.103:9092");
        properties.put("acks","all");
        properties.put("retries",1);
        properties.put("batch.size",16384);
        properties.put("linger.ms",1);
        properties.put("buffer.memory",33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<>("test","3"));
        producer.close();


    }

    //带回调函数的 API
    @Test
    public void test02(){
        Properties properties=new Properties();
        properties.put("bootstrap.servers","192.168.226.103:9092");
        properties.put("acks","all");
        properties.put("retries",1);
        properties.put("batch.size",16384);
        properties.put("linger.ms",1);
        properties.put("buffer.memory",33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        //增加自定义拦截器----begin
        List<String> list=new ArrayList<>();
        list.add("com.rxl.utils.interceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list);
        //增加自定义拦截器----end
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<>("test", "34"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                 if(ObjectUtils.isEmpty(e)){   //发送成功
                     System.out.println("消息发送成"+recordMetadata.offset());
                 }else{
                     e.printStackTrace();
                 }
            }
        });
        producer.close();
    }


    /**
     *
     */
    @Test
    public void Test03(){
        Properties properties=new Properties();
        properties.put("bootstrap.servers","192.168.226.103:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "false");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        //创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        });

    }

    //提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
