package com.pingan.examine

import java.io.{File, FileOutputStream, OutputStreamWriter}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.pingan.examine.start.ConfigFactory
import com.pingan.examine.utils.KafkaUtil
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/12/15.
  */
/*数据反馈类
* 该类主要用于把数据返回给接口调用
* 并把数据做本系统的持久化保存
* */
class DataFeedback {
  /*处理过滤后的结果集
  * 参数：传入的原始数据，通过过滤的数据
  * */
  def handleResultData(sourceData:RDD[String],adoptData:RDD[String]):Unit={
    var adoptArray:Array[String] = null;
    var noadoptArray:Array[String] = null;
    if(adoptData==null) {
      sourceData.foreach(rdd => {
        val d504bean: JSONObject = JSON.parseObject(rdd)
        //主题，医院id，key:就诊流水号，value:审核结果
        KafkaUtil.sendDataToKafka(d504bean.get("d504_14").toString, d504bean.get("d504_01").toString
          , "error")
      })
      //将sourceData全赋给noadoptArray
      noadoptArray = sourceData.collect()
    }else{
      //将原始数据与通过的之间求差集
      val noadoptData:RDD[String] = sourceData.subtract(adoptData)
      adoptData.foreach(rdd=>{
        //将rdd转换为JSON类型数据
        val d504bean:JSONObject = JSON.parseObject(rdd)
        println("发送审核通过数据到kafka")
        //从通过的主题，key，value提取出来
        KafkaUtil.sendDataToKafka(d504bean.get("d504_14").toString,d504bean.get("d504_01").toString,
        "success")
      })
        noadoptData.foreach(rdd => {
          val d504bean: JSONObject = JSON.parseObject(rdd)
          println("发送审核未通过数据到kafka")
          KafkaUtil.sendDataToKafka(d504bean.get("d504_14").toString, d504bean.get("d504_01").toString
            , "error")
        })
      adoptArray = adoptData.collect()
      noadoptArray = noadoptData.collect()
    }
    //对于得出的数据进行文件保存
    val filename = System.currentTimeMillis()
    writeToLocal(ConfigFactory.localpath+File.separator+"adopt"+File.separator+filename+".txt",
      adoptArray)
    writeToLocal(ConfigFactory.localpath+File.separator+"noadopt"+File.separator+filename+".txt",
      noadoptArray)
  }
  /*保存内容到文件
  参数：path,dataArray
  *
  * */
  private def writeToLocal(path:String,dataArray:Array[String]):Unit={
    if(dataArray==null || dataArray.length<1){
      return
    }
    println("保存文件:"+path)
    val tempFile:File = new File(path)
    if(tempFile.createNewFile()){
      //往指定的文件里写
      val out = new FileOutputStream(path,true)
      val writer = new OutputStreamWriter(out,"utf-8")
      for(cnt<- 0 until dataArray.length){
        writer.write(dataArray(cnt)+"\r\n")
      }
      writer.flush()
      out.close()
    }
  }

}
