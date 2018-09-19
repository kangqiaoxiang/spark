package com.pingan.examine.utils;

import com.pingan.examine.start.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Administrator on 2017/12/15.
 */
/*kafka工具类
*
* */
public class KafkaUtil {
    private static KafkaProducer<String,String> producer = null;
    /*发送消息到kafka
    参数：医院id,就诊流水号，审核结果（通过success,未通过error)
    * */
    public static void sendDataToKafka(String topic,String key,String value){
        if(producer == null){
            initKafkaProducer();
        }
        ProducerRecord<String,String> message = new ProducerRecord(topic,key,value);
        producer.send(message);
    }
    /*初始化producer对象
    * */
    private static void initKafkaProducer(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", ConfigFactory.kafkaip+":"+ConfigFactory.kafkaport);
        prop.put("acks","0");
        prop.put("retries",0);
        prop.put("batch.size",16384);
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(prop);

    }
}
