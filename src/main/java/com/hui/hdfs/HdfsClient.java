package com.hui.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
}
