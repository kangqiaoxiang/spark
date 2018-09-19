package com.pingan.examine;

import com.pingan.examine.start.ConfigFactory;
import org.junit.Test;



/**
 * Created by Administrator on 2017/12/14.
 */
public class DataInputTest {
    @Test
    public void Data(){
        ConfigFactory.initConfig();
        DataInput dataInput=new DataInput();
        System.setProperty("hadoop.home.dir","E:\\yj\\hadoop-2.7.2\\hadoop-2.7.2");
        dataInput.getDataForKafka();
    }

}
