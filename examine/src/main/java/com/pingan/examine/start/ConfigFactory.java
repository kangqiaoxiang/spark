package com.pingan.examine.start;
import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * 通用配置文件加载类
 * Created by captain on 2017/10/31.
 */
public class ConfigFactory {

    private final static Logger log = LogManager.getLogger("ConfigFactory");

    public static String kafkaip; //kafkaip

    public static String kafkaport; //kafka端口号

    public static String kafkazookeeper;  //kafkazookeeper集群地址

    public static String mysqlurl; //mysql连接信息

    public static String mysqlusername; //mysql用户名

    public static String mysqlpasswd; //mysql密码

    public static String sparkstreammaster; //stream运行模式

    public static String sparkstreamname; //stream任务名称

    public static int sparkseconds; //stream时间间隔

    public static String sparksqlmaster; //sql运行模式

    public static String sparksqlname; //stream任务名称

    public static String localpath; //本地保存路径

    public static String hdfspath; //hdfs保存路径

    public static Configuration hadoopconf;  //hadoop配置文件

    /**
     * 初始化所有的通用信息
     */
    public static void initConfig(){
        readCommons();
    }

    /**
     * 读取commons.xml，加载到内存
     */
    private static void readCommons(){
        SAXReader reader = new SAXReader();  //构建XML解析器
        Document document = null;  //读取文件
        try {
            document = reader.read(Resources.getResource("commons.xml"));
        } catch (DocumentException e) {
            log.error("ConfigFactory.readCommons",e);
        }
        if(document!=null){
            Element root = document.getRootElement();  //获取root节点

            Element kafkaElement = root.element("kafka");  //获取kafka连接信息
            kafkaip = kafkaElement.element("ip").getText();
            kafkaport = kafkaElement.element("port").getText();
            kafkazookeeper = kafkaElement.element("zookeeper").getText();

            Element mysqlElement = root.element("mysql");  //获取mysql连接信息
            mysqlurl = mysqlElement.element("url").getText();
            mysqlusername = mysqlElement.element("username").getText();
            mysqlpasswd = mysqlElement.element("passwd").getText();

            Element sparkElement = root.element("spark");  //获取spark配置信息
            sparkstreammaster = sparkElement.element("streammaster").getText();
            sparkstreamname = sparkElement.element("streamname").getText();
            sparkseconds = Integer.valueOf(sparkElement.element("seconds").getText());
            sparksqlmaster = sparkElement.element("sqlmaster").getText();
            sparksqlname = sparkElement.element("sqlname").getText();

            Element pathElement = root.element("path");  //获取spark配置信息
            localpath = pathElement.element("localpath").getText();
            hdfspath = pathElement.element("hdfspath").getText();

            hadoopconf = new Configuration();
            hadoopconf.addResource(Resources.getResource("core-site.xml"));
        }else{
            log.warn("commons.xml配置文件读取错误");
        }
    }

}
