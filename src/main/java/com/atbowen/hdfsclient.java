package com.atbowen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @ProjectName: hdfs
 * @Package: com.atbowen
 * @ClassName: hdfsclient
 * @Author: Bowen
 * @Description: HDFS客户端
 * @Date: 2019/9/29 12:11
 * @Version: 1.0.0
 */
public class hdfsclient {

    @Test
    public void put() throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop2:9000"), new Configuration(), "hadoop");

        fileSystem.copyFromLocalFile(new Path("D:\\BIGDATA\\input\\1.txt"), new Path("/"));

        fileSystem.close();
    }

    @Test
    public void rename() throws IOException, InterruptedException {
        FileSystem hadoop = FileSystem.get(URI.create("hdfs://hadoop2:9000"), new Configuration(), "hadoop");
        hadoop.rename(new Path("/1.txt"), new Path("/2.txt"));
        hadoop.close();
    }

    @Test
    public void delete() throws IOException, InterruptedException {
        FileSystem hadoop = FileSystem.get(URI.create("hdfs://hadoop2:9000"), new Configuration(), "hadoop");
        boolean delete = hadoop.delete(new Path("/2.txt"), true);
        if (delete) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
        hadoop.close();
    }
}
