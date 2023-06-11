package com.airing.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 以hadoop jar包的方式运行
 *
 * @author GEYI
 * @date 2023年06月11日 21:28
 */
public class MyWordCount {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyWordCount.class);
        job.setJobName("MyWordCount");

        Path infile = new Path("/data/wc/input");
        TextInputFormat.addInputPath(job, infile);
        Path outfile = new Path("/data/wc/output");
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        TextOutputFormat.setOutputPath(job, outfile);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);
    }

}
