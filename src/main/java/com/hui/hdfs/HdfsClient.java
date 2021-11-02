package com.hui.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException{
        //1、获取文件系统
        Configuration configuration = new Configuration();

        // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration,"zhanghui");

        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));

        // 3 关闭资源
        fs.close();

    }

    @Test
    public void testCopyFromLocalFile()throws IOException, InterruptedException, URISyntaxException{
        //1、获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "zhanghui");

        //2、上传文件
        fs.copyFromLocalFile(new Path("D:/尚硅谷/尚硅谷大数据技术之Hadoop3.x/word.txt"),new Path("/xiyou/huaguoshan"));

        //3、关闭资源
        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

        //1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "zhanghui");

        //2、执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验

        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan/word.txt"),new Path("D:/尚硅谷/尚硅谷大数据技术之Hadoop3.x/word2.txt"),true);

        //3、关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{

        //1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "zhanghui");

        //2、修改文件名称

        fs.rename(new Path("/xiyou/huaguoshan/word.txt"),new Path("/xiyou/huaguoshan/meihouwang.txt"));

        //3、关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{

        //1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "zhanghui");

        //2、修改文件名称

        fs.delete(new Path("/xiyou"),true);

        //3、关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

        //1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "zhanghui");

        //2、获取文件详情

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========"+fileStatus.getPath()+"========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        //3、关闭资源
        fs.close();
    }
}
