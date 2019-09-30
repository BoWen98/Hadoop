package com.atbowen.wordcunt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ProjectName: hdfs
 * @Package: com.atbowen.wordcunt
 * @ClassName: WcMapper
 * @Author: Bowen
 * @Description: Mapper
 * @Date: 2019/9/30 14:45
 * @Version: 1.0.0
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();

        String[] s1 = s.split(" ");

        for (String s2 : s1) {
            this.word.set(s2);
            context.write(this.word, this.one);
        }
    }
}
