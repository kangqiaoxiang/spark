package com.pingan.examine.bean;
import java.util.List;
/**
 * Created by Administrator on 2017/12/14.
 */
public class RecordBean {
    private D504Bean d504Bean;
    private List<D505Bean> d505BeanList;
    public RecordBean(){

    }
    //为什么构造这么多次？
    public RecordBean(D504Bean d504Bean, List<D505Bean> d505BeanList){
        this.d504Bean = d504Bean;
        this.d505BeanList = d505BeanList;
    }

    public List<D505Bean> getD505BeanList() {
        return d505BeanList;
    }

    public D504Bean getD504Bean(){
        return d504Bean;
    }
    public void setD504Bean(D504Bean d504Bean){
        this.d504Bean = d504Bean;
    }
    public void setD505BeanList(List<D505Bean> d505BeanList){
        this.d505BeanList = d505BeanList;
    }
}
