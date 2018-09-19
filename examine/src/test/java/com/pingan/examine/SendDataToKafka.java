package com.pingan.examine;
import com.alibaba.fastjson.JSON;
import com.pingan.examine.bean.D504Bean;
import com.pingan.examine.bean.D505Bean;
import com.pingan.examine.bean.RecordBean;
import com.pingan.examine.start.ConfigFactory;
import org.apache.commons.lang.StringUtils;
import jodd.util.ThreadUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * Created by Administrator on 2017/12/14.
 */
public class SendDataToKafka {
    @Test
    public void SendData() throws IOException {

        ConfigFactory.initConfig();
        List<D504Bean> d504Beanlist = getD504Bean();
        Map<String, List<D505Bean>> d504Beanmap = getD505Bean();
        //创建生产者
        Properties prop = new Properties();
        prop.put("bootstrap.servers", ConfigFactory.kafkaip + ":" + ConfigFactory.kafkaport);
        prop.put("acks", "0");
        prop.put("retries", 0);
        prop.put("batch.size", 16384);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(prop);
        int cnt = 0;
        while (true) {
            D504Bean d504Bean = d504Beanlist.get(cnt);
            RecordBean bean = new RecordBean(d504Bean, d504Beanmap.get(d504Bean.getD504_01()));
            String datamessage = JSON.toJSONString(bean);
            ProducerRecord<String, String> message = new ProducerRecord("reimbursement-message", String.valueOf(cnt), datamessage
            );
            kafkaProducer.send(message);
            System.out.println("发送消息："+cnt+ ","+datamessage);
            ThreadUtil.sleep(1000*2);
            cnt++;
            if(cnt==d504Beanlist.size()){
                cnt = 0;
            }
        }
    }

    /**读取测试数据文件到内存
     * filename:要读取的文件
     * 读取到的文件，并按照逗号拆分
     *
     */

    private List<List<String>> getSourceData(String filename)throws IOException{
        //从文件读取数据
        List<List<String>> returnValue = new ArrayList<List<String>>();
        InputStream inputStream = new FileInputStream("D:\\logs\\"+filename);
        Reader reader = new InputStreamReader(inputStream, "utf-8");
        LineNumberReader lnr = new LineNumberReader(reader);
        while (true) {
            String str = lnr.readLine();
            if (str == null) {
                break;
            }
            if (StringUtils.startsWith(str, "values")) {
                List<String> list = new ArrayList<String>();
                str = str.substring(8, str.length() - 2);
                String[] array = str.split("to_date\\(");
                list.addAll(Arrays.asList(array[0].split(",")));
                if (list.get(list.size() - 1).equals(" ")) {
                    list.remove(list.size() - 1);
                }
                for (int cnt= 1; cnt< array.length; cnt++) {
                    List<String> tempList = new ArrayList<String>(Arrays.asList(array[cnt].split(",")));
                    tempList.remove(1);
                    if (tempList.get(tempList.size() - 1).equals(" ")) {
                        tempList.remove(tempList.size() - 1);
                    }
                    list.addAll(tempList);
                }
                returnValue.add(list);
            }

        }
        inputStream.close();
        return returnValue;

    }
    /*获取所有的D504Bean
    * return：返回组装好的D504Bean
    * */
    private List<D504Bean> getD504Bean() throws IOException {
        List<List<String>> d504List = getSourceData("d504.sql");
        List<D504Bean> returnvalue = new ArrayList<D504Bean>(d504List.size());
        for(List<String> oneData:d504List){
            if(oneData!=null&&oneData.size()==65){
                List<String> newList = new ArrayList<String>(oneData.size());
                for(String str:oneData){
                    newList.add(str.trim().replaceAll("'",""));
                }
                D504Bean bean = new D504Bean(newList);
                returnvalue.add(bean);
            }
        }
        return returnvalue;
    }
    /*获取所有的D505Bean
    * return:组装好的D505Bean
    * */
    private Map<String,List<D505Bean>> getD505Bean()throws IOException{
        List<List<String>> d505list = getSourceData("d505.sql");
        Map<String,List<D505Bean>> returnvalue = new HashMap(d505list.size());
        for(List<String> oneData:d505list){
            if(oneData!=null&&oneData.size()==44){
                List<String> newList = new ArrayList<String>(oneData.size());
                for(String str:oneData){
                    newList.add(str.trim().replaceAll("'",""));
                }
                D505Bean bean = new D505Bean(newList);
                if(returnvalue.get(bean.getD505_01())==null){
                    List<D505Bean> tempList = new ArrayList<D505Bean>();
                    tempList.add(bean);
                    returnvalue.put(bean.getD505_01(),tempList);
                }else{
                    List<D505Bean> tempList = returnvalue.get(bean.getD505_01());
                    tempList.add(bean);
                    returnvalue.put(bean.getD505_01(),tempList);
                }
            }
        }
        return returnvalue;

    }


}
