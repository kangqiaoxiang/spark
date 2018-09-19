package com.pingan.examine.utils;

import jodd.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

/**
 * hdfs通用工具类
 * Created by captain on 2017/3/3.
 */
public class HdfsUtil {

    /**
     * 判断目录或文件是否存在
     * @param conf Configuration对象
     * @param hdfsPath 要检测的完整路径
     * @return 如果存在返回true，否则返回false
     * @throws IOException
     */
    public static boolean existsFiles(Configuration conf, String hdfsPath) throws IOException {
        if(conf==null|| StringUtil.isEmpty(hdfsPath))
            return false;
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        boolean flag = fileSystem.exists(path);
        fileSystem.close();
        return flag;
    }

    /**
     * 创建一个文件夹，支持递归创建，已存在直接返回true
     * @param conf Configuration对象
     * @param hdfsPath 要创建的文件夹完整路径
     * @return 创建成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean createFolder(Configuration conf, String hdfsPath) throws IOException {
        if(conf==null|| StringUtil.isEmpty(hdfsPath))
            return false;
        if(existsFiles(conf,hdfsPath))
            return true;
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        boolean flag = fileSystem.mkdirs(path);
        fileSystem.close();
        return flag;
    }

    /**
     * 创建一个文件，支持递归创建，已存在直接返回true,不改变原文件
     * @param conf Configuration对象
     * @param hdfsPath 要创建的文件夹完整路径
     * @return 创建成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean createFile(Configuration conf, String hdfsPath) throws IOException {
        if(conf==null|| StringUtil.isEmpty(hdfsPath))
            return false;
        if(existsFiles(conf,hdfsPath))
            return true;
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        fileSystem.create(path);
        fileSystem.close();
        return true;
    }

    /**
     * 追加内容到文件
     * @param conf Configuration对象
     * @param hdfsPath 要追加的文件完整路径
     * @param value 要追加的内容
     * @return 追加成功返回true，文件不存在或追加失败返回false
     * @throws IOException
     */
    public static boolean writeFile(Configuration conf, String hdfsPath, String value) throws IOException {
        if(conf==null||StringUtil.isEmpty(hdfsPath)||StringUtil.isEmpty(value))
            return false;
        if(!existsFiles(conf,hdfsPath))
            return false;
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        FSDataOutputStream fsDataOutputStream = fileSystem.append(path);
        byte[] btyes = value.getBytes();
        fsDataOutputStream.write(btyes);
        fsDataOutputStream.close();
        fileSystem.close();
        return true;
    }

    /**
     * 读取一个文件
     * @param conf Configuration对象
     * @param hdfsPath 要读取的文件完整路径
     * @return 读取到的文件内容，文件不存在或不是文件返回null
     * @throws IOException
     */
    public static List<String> readFile(Configuration conf, String hdfsPath) throws IOException {
        if(conf==null|| StringUtil.isEmpty(hdfsPath))
            return null;
        if(!existsFiles(conf,hdfsPath))
            return null;
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        List<String> returnValue = null;
        if(fileSystem.isFile(path)){
        	FSDataInputStream fsDataInputStream = fileSystem.open(path);
            InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream);
            LineNumberReader lineNumberReader = new LineNumberReader(inputStreamReader);
            String tempStr;
            returnValue = new ArrayList<>(lineNumberReader.getLineNumber());
            while((tempStr = lineNumberReader.readLine())!=null){
                returnValue.add(tempStr);
            }
            lineNumberReader.close();
            inputStreamReader.close();
            fsDataInputStream.close();
        }
        fileSystem.close();
        return returnValue;
    }

    /**
     * 上传本地文件到hdfs
     * @param conf Configuration
     * @param localFile 本地文件完整路径
     * @param hdfsFolder hdfs文件夹完整路径
     * @return 上传成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean localFileUploadHDFS(Configuration conf, String localFile, String hdfsFolder) throws IOException {
        if(conf==null||StringUtil.isEmpty(localFile)||StringUtil.isEmpty(hdfsFolder))
            return false;
        if(!existsFiles(conf,hdfsFolder))
            return false;
        File file = new File(localFile);
        if(!file.exists()||file.isDirectory())
            return false;
        FileSystem fileSystem = FileSystem.get(conf);
        Path pathhdfs = new Path(hdfsFolder);
        Path pathlocal = new Path(localFile);
        fileSystem.copyFromLocalFile(pathlocal,pathhdfs);
        fileSystem.close();
        return true;
    }

    /**
     * 上传本地文件夹中所有文件到hdfs
     * @param conf Configuration
     * @param localFolder 本地文件夹完整路径
     * @param hdfsFolder hdfs文件夹完整路径
     * @return 上传成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean localFolderUploadHDFS(Configuration conf, String localFolder, String hdfsFolder) throws IOException {
        if(conf==null||StringUtil.isEmpty(localFolder)||StringUtil.isEmpty(hdfsFolder))
            return false;
        if(!existsFiles(conf,hdfsFolder))
            return false;
        File file = new File(localFolder);
        if(!file.exists()||file.isFile())
            return false;
        File[] files = file.listFiles();
        if(files==null||files.length<1)
            return false;
        FileSystem fileSystem = FileSystem.get(conf);
        Path pathhdfs = new Path(hdfsFolder);
        for(File tempFile : files){
            if(tempFile.isDirectory())
                continue;
            fileSystem.copyFromLocalFile(new Path(tempFile.getPath()),pathhdfs);
        }
        fileSystem.close();
        return true;
    }

    /**
     * 下载HDFS文件或文件夹到本地
     * @param conf Configuration
     * @param localFolder 本地文件夹完整路径
     * @param hdfsFile hdfs文件或文件夹完整路径
     * @return 下载成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean hdfsDownLocal(Configuration conf, String localFolder, String hdfsFile) throws IOException {
        if(conf==null||StringUtil.isEmpty(localFolder)||StringUtil.isEmpty(hdfsFile))
            return false;
        File file = new File(localFolder);
        if(!file.exists()||file.isFile())
            return false;
        if(!existsFiles(conf,hdfsFile))
            return false;
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsFile);
        fileSystem.copyToLocalFile(path,new Path(localFolder));
        return true;
    }
    
    /**
     * 移动文件到指定位置
     * @param conf Configuration
     * @param inputpath 原始文件位置
     * @param outputpath 指定文件位置
     * @return 移动成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean moveDataToHdfs(Configuration conf, String inputpath, String outputpath) throws IOException{
    	if(conf==null||StringUtil.isEmpty(inputpath)||StringUtil.isEmpty(outputpath))
            return false;
        FileSystem fileSystem = FileSystem.get(conf);
        return fileSystem.rename(new Path(inputpath), new Path(outputpath));
    }

    /**
     * 删除文件或文件夹
     * 如果文件夹不为空，则递归删除
     * @param conf Configuration对象
     * @param hdfsPath 要删除的文件或文件夹完整路径
     * @return 删除成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean deleteFolder(Configuration conf, String hdfsPath) throws IOException {
        if(conf==null||StringUtil.isEmpty(hdfsPath))
            return false;
        if(!existsFiles(conf,hdfsPath))
            return false;
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        fileSystem.delete(path,true);
        fileSystem.close();
        return true;
    }

}
