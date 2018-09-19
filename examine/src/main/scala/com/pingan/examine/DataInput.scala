package com.pingan.examine

import com.alibaba.fastjson.{JSON, JSONObject}
import com.pingan.examine.start.ConfigFactory
import jodd.util.StringUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Timer

import com.pingan.examine.utils.UpdateHdfsTask;




/**数据获取类
  * Created by Administrator on 2017/12/11.
  */
class DataInput {
  private val dataFeedback:DataFeedback = new DataFeedback
  /*从kafka获取医疗机构要审核的报销信息
  *
  * */

  def getDataForKafka(): Unit = {
    val conf1 = new SparkConf();
    conf1.setMaster(ConfigFactory.sparkstreammaster).setAppName(ConfigFactory.sparkstreamname)
    conf1.set("spark.testing.memory","21474800000")
    val streamingContext = new StreamingContext(conf1, Seconds(ConfigFactory.sparkseconds))
    val sparkSession = SparkSession.builder()
    sparkSession.master(ConfigFactory.sparksqlmaster).appName(ConfigFactory.sparksqlname)
    val jsonSession:SparkSession = sparkSession.getOrCreate()
    //正式运行 jsonSession.read.json("hdfs://examine/hisd504")
    /*val hisd504DataFrame:DataFrame=jsonSession.read.json("hdfs://master1.hadoop:/examine/hisd504")
    *val d506DataFrame:DataFrame = jsonSession.read.json("hdfs://master1.hadoop:9000/examine/d506")
    * hisd504DataFrame.createOrReplaceTempView("hisd504table")
    * d506DataFrame.createOrReplaceTempView("d506table")
    * */
    val rmessageRdd: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, ConfigFactory.kafkazookeeper, "examine-group",
      Map("reimbursement-message" -> 1), StorageLevel.MEMORY_ONLY)
    //rmessageRdd.print()
    rmessageRdd.foreachRDD(rdd => {
      rdd.persist()
      val count = rdd.count()
      println("本次处理数据数量:" + count)
      if (count > 0) {
        getDataFrameForDStream(rdd, sparkSession,jsonSession)
      }
    })
    //TODO审核报销信息，并做相应保存和返回工作
    startTimeTask()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  //Kafaka接收json数据，将DStream转换为DataFrame
  //def getDataFrameForDStream(data:ReceiverInputDStream[(String,String)]):DataFrame= {
  //    val dataValues = data.map { case (key, value) => value }
  //    var allValues: RDD[String] = null
  //    dataValues.foreachRDD(rdd => allValues = allValues.union(rdd))
  //    //变成RDD，可以让JSON接收,创建SparkSession接收数据
  //    val sparkSession = SparkSession.builder()
  //    sparkSession.master(ConfigFactory.sparksqlmaster).appName(ConfigFactory.sparksqlname)
  //    val jsonsession = sparkSession.getOrCreate()
  //    val tempRdd:DataFrame = jsonsession.read.json( allValues)
  //    return tempRdd
  /*
  * 将原始RDD[(String,String)]转换为RDD[String],RDD中的每行都是一个完整的bean的json
  * 返回：第一个元素为D504表的rdd，第二个为D505表的rdd
  * */
  private def getDataFrame(rdd: RDD[(String, String)]): List[RDD[String]] = {
    //将RDD[(String,String)]转换为RDD[String]
    val d504Rdd: RDD[String] = rdd.map { case (key, value) =>
      val beanjson: JSONObject = JSON.parseObject(value)
      //转换后根据键获取值
      beanjson.get("d504Bean").toString()
    }
    val d505Rdd: RDD[String] = rdd.flatMap { case (key, value) =>
      val beanjson: JSONObject = JSON.parseObject(value)
      var d505BeanList = beanjson.get("d505BeanList").toString()
      d505BeanList = d505BeanList.substring(1, d505BeanList.length() - 1)
      val d505Array = StringUtil.split(d505BeanList, "},{")
      d505Array(0) = d505Array(0) + "}"
      d505Array(d505Array.length - 1) = "{" + d505Array(d505Array.length - 1)
      if (d505Array.length > 2) {
        var cnt = 1;
        for (cnt <- 1 to d505Array.length - 1) {
          d505Array(cnt) = "{" + d505Array(cnt) + "}"
        }
      }
      d505Array
    }
    //从kafka接收数据后，对数据进行处理
    List(d504Rdd, d505Rdd)


  }

  /*处理单个Dstream中的RDD
参数：本次要处理的RDD，sparkSession SparkSession.Builder
  *
  * */
  //
  private def getDataFrameForDStream(rdd: RDD[(String, String)], sparkSession: SparkSession.Builder,
                                     jsonSession:SparkSession): Unit = {
    val rdds: List[RDD[String]] = getDataFrame(rdd)
    val d504DataFrame = jsonSession.read.json(rdds(0))
    val d505DataFrame = jsonSession.read.json(rdds(1))
    d505DataFrame.persist()
    d504DataFrame.createOrReplaceTempView("d504table")
    d505DataFrame.createOrReplaceTempView("d505table")
    //第一次过滤后生成的newD504Rdd
    val newD504Rdd: RDD[String] = mainExamine(jsonSession, rdds(0))
    if (newD504Rdd == null) {
      println("跳过第二次过滤，没有要处理的数据")
    } else {
      //进行第二次过滤
      val newd504DataFrame: DataFrame = jsonSession.read.json(newD504Rdd)
      newd504DataFrame.createOrReplaceTempView("d504table")
      val endAdoptRdd: RDD[String] = repeatExamine(jsonSession, newD504Rdd)
     //endAdoptRdd.foreach(println)
      dataFeedback.handleResultData(rdds(0),endAdoptRdd)
    }

  }

  /*住院主审核过滤程序
  参数：jsonSession SparkSession,d504rdd D504表的rdd
  *返回过滤之后通过审核的新d504rdd
  * */
  private def mainExamine(jsonSession: SparkSession, d504rdd: RDD[String]): RDD[String] = {
    //测试数据没有小于3的，这里暂时改为15
    // 这里过来出来的是合格的
    val sql = "select t1.d504_01,count(t1.d504_01) as cnt " +
      "from d504table t1,d505table t2" +
      " where t1.d504_13 <= 14" +
      " and t2.e505_02 = 0" +
      " and t1.d504_01 = t2.d505_01" +
      " and t1.d504_12 = t2.d505_13" +
      " group by t1.d504_01 having cnt <= 15"
    //val rows:DataFrame = jsonSession.sql(sql)
    // var adoptList:List[String] = List()
    //    rows.foreach(row=>{
    //      adoptList = adoptList ++ List(row.getString(0))
    //    })
    //    val newd504rdd:RDD[String] = d504rdd.filter(line=>{
    //        val d504id = JSON.parseObject(line).get("d504_01").toString()
    //        if(adoptList.contains(d504id)){
    //          true
    //        }else{
    //          false
    //        }
    //      })
    //    newd504rdd
    //  }
    //合格的数据
    val rows: DataFrame = jsonSession.sql(sql)
    println("++++++第一次过滤结果++++++")
    rows.show(1000)
    println("——————————————————")
    val rowArray = rows.collect()
    //创建一个空的List集合，获得通过检测的d504_01,住院登记流水号
    var adoptList: List[String] = List()
    for (cnt <- 0 until rowArray.length) {
      adoptList = adoptList ++: List(rowArray(cnt).getString(0))
    }
    //有可能全部没有通过
    if (adoptList.size < 1) {
      return null
    } else {
      //对原有的rdd进行过滤，返回通过检测的rdd
      val newd504rdd: RDD[String] = d504rdd.filter(line => {
        val d504id = JSON.parseObject(line).get("d504_01").toString
        if (adoptList.contains(d504id)) {
          true
        } else {
          false
        }
      })
      return newd504rdd
    }

  }


  /*重复住院过滤
  参数：jsonSession:SparkSession,newD504Rdd ：第一次过滤后剩余的符合条件的记录集合
  返回：第二次过滤后剩余的符合条件的记录集合
  * */
  private def repeatExamine(jsonSession: SparkSession, newD504Rdd
  : RDD[String]): RDD[String] = {
    //正式运行  jsonSession.read.json("hdfs://examine/hisd504")
    val hisdD504DataFrame: DataFrame = jsonSession.read.json("hisd504.txt")
    val d506DataFrame: DataFrame = jsonSession.read.json("d506.txt")
    hisdD504DataFrame.createOrReplaceTempView("hisd504table")
    d506DataFrame.createOrReplaceTempView("d506table")
    //    val sql = "select t1.504_01 " +
    //      "from d504table t1,d504table t2,hisd504table t3,d506table t4" +
    //      "where t1.d504_01 = t2.d505_01 and t1.d504_02 = t3.d505_02" +
    //      "and t1.d504_02=t4.e506_01 " +
    //      "and (" +
    //      "(t1.D504_14 = t3.D04_14 and (case(t1.D504_11 as long)-" +
    //      "case(t3.D504_12 as long)) <=7 and t1.D504_21 = t3.D504_21 and " +
    //      "(t3.D504_20 = '0' or t3.D504_20 = '')) or
    val sql =  """select distinct t1.d504_01 from d504table t1,d505table t2,hisd504table t3,d506table t4
                 |where t1.d504_01=t2.d505_01 and t1.d504_02=t3.d504_02 and t1.d504_02=t4.e506_01
                 |and(
                 |(t1.d504_14=t3.d504_14 and CAST(UNIX_TIMESTAMP(t1.d504_11,'dd-MM-yyyy')
                 |AS LONG)-CAST(UNIX_TIMESTAMP(t3.d504_12,
                 |'dd-MM-yyyy')AS LONG)<=604800000)
                 |or (t1.d504_14!=t3.d504_14 and CAST(UNIX_TIMESTAMP(t1.d504_11,'dd-MM-yyyy')
                 |AS LONG)-CAST(UNIX_TIMESTAMP(t3.d504_12,'dd-MM-yyyy')AS LONG)<=604800000
                 |and t1.d504_21=t3.d504_21 and (t3.d504_20='0' or t3.d504_20=''))
                 |or ((select sum(d506_24) from d506table  where e506_01=t1.d504_02 group
                 |by e506_01)+(select sum(t5.e505_06) from d505table t5 where t5.d505_01 = t1.d504_01 group by t5.d505_01)>300000)
                 |or t4.d506_25!=0)
               """.stripMargin

    val rows: DataFrame = jsonSession.sql(sql)
    println("+++++++第二次过滤结果++++++")
    rows.show(1000)
    print("————————————————")
    val rowArray = rows.collect()
    var adoptList: List[String] = List()
    for (cnt <- 0 until rowArray.length) {
      adoptList = adoptList ++: List(rowArray(cnt).getString(0))
    }
    val endd504rdd: RDD[String] = newD504Rdd.filter(line => {
      val d504id = JSON.parseObject(line).get("d504_01").toString()
      if (adoptList.contains(d504id)) {
        false
      } else {
        true
      }
    })
    endd504rdd

  }

//    var adoptList:List[String] = List()
//    rows.foreach(row =>{
//      adoptList = adoptList ++ List(row.getString(0))
//    })
//    val endd504rdd:RDD[String] = newD504Rdd.filter(line=>{
//      val d504id = JSON.parseObject(line).get("d504_01").toString()
//      if(adoptList.contains(d504id)){
//        false
//      }else{
//        true
//      }
//    })
//    endd504rdd
//  }
  /*启动定时器定期上传文件到hdfs
    * */
  private def startTimeTask():Unit={
  val timer = new Timer();
  timer.schedule(new UpdateHdfsTask(),0,1000*60);
  }
}





