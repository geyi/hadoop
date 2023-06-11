package com.airing.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 本地运行
 *
 * @author GEYI
 * @date 2023年06月11日 21:28
 */
public class MyWordCount3 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyWordCount3.class);
        job.setJobName("MyWordCount3");

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
