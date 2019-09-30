package com.atbowen.wordcunt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ProjectName: hdfs
 * @Package: com.atbowen.wordcunt
 * @ClassName: WcDriver
 * @Author: Bowen
 * @Description: Driver
 * @Date: 2019/9/30 14:58
 * @Version: 1.0.0
 */
public class WcDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取一个JOB实例
        Job job = Job.getInstance(new Configuration());

        //2.设置我们类路径
        job.setJarByClass(WcDriver.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //4.设置Mapper和Driver输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //5.设置输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交我们Job
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);

    }
}
