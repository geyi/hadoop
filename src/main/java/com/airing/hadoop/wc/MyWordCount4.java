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
 * 参数个性化
 *
 * @author GEYI
 * @date 2023年06月11日 21:28
 */
public class MyWordCount4 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] othargs = parser.getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyWordCount4.class);
        job.setJobName("MyWordCount4");

        Path infile = new Path(othargs[0]);
        TextInputFormat.addInputPath(job, infile);
        Path outfile = new Path(othargs[1]);
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
