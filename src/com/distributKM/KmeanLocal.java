package com.distributKM;


import java.io.*;
import java.util.List;

import com.distributKM.sift.SiftDescriptor;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.jetty.HttpParser;

/**
 * Created by lichaochen on 15/5/14.
 */
public class KmeanLocal {

    public static class KmeansMaper
            extends Mapper<Object, Text, IntWritable, Text >{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            /*collect centers from file*/

            List <SiftDescriptor> centerCluster  ;
            centerCluster = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream( new File("KM_center/centers")));

            /* find corresponding center for given sift descriptor */

            String siftLine = value.toString();
            SiftDescriptor sift = new SiftDescriptor(siftLine);
            SiftDescriptor center = sift.findMostSimilar(centerCluster);

            int centerKey = centerCluster.indexOf(center);

            context.write(new IntWritable(centerKey) ,value);

        }

    }


    public static class KmeansReducer
            extends Reducer<IntWritable,Text, IntWritable, Text> {

        private Text newCenterText = new Text();
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // find new Center for sift cluster
            SiftDescriptor newCenter = new SiftDescriptor();
            int loop =0;
            for (Text siftLine : values) {
                newCenter.add(new SiftDescriptor(siftLine.toString()));
                loop++;
            }
            newCenter.shrink(loop);
            newCenterText.set(newCenter.toString());
            context.write(key, newCenterText);
        }
    }

    public static Job getMapReduceJob(String[] args) throws IOException {

        Job job = Job.getInstance();
        job.setJobName("DistributedKM");
        job.setJarByClass(KmeanLocal.class);
        job.setMapperClass(KmeansMaper.class);
        // job.setCombinerClass(KmeansReducer.class);
        job.setReducerClass(KmeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        Configuration conf = job.getConfiguration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Kmean <in> [<in>...] <out>");
            System.exit(2);
        }

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        return job;
    }

    public static void main(String[] args) throws Exception {


        int MAX_LOOP = 1000;
        boolean isCenterFixed = false;
        int loop = 0;
        while(loop<MAX_LOOP && !isCenterFixed ){
            Job job = getMapReduceJob(args);

            job.waitForCompletion(true);

            File outputF = new File("KM_output/part-r-00000");
            File centerF = new File("KM_center/centers");

            List <SiftDescriptor> newCenter = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream(outputF));
            List <SiftDescriptor> oldCenter = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream(centerF));

            isCenterFixed = SiftDescriptor.isClusterDistanceLessThenThreshold(newCenter,oldCenter,100);

            outputF.renameTo(centerF);
            System.out.println("again!!!----" + loop);
            new File("KM_output/._SUCCESS.crc").delete();
            new File("KM_output/.part-r-00000.crc").delete();
            new File("KM_output/_SUCCESS").delete();
            new File("KM_output").delete();
            loop ++;
        }



//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
