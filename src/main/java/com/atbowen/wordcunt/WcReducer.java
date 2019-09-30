package com.atbowen.wordcunt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ProjectName: hdfs
 * @Package: com.atbowen.wordcunt
 * @ClassName: WcReducer
 * @Author: Bowen
 * @Description: Reducet
 * @Date: 2019/9/30 14:51
 * @Version: 1.0.0
 */
public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable toal = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        toal.set(sum);
        context.write(key, toal);
    }
}
