package com.distributKM;


import com.distributKM.sift.SiftDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lichaochen on 15/5/14.
 */
public class Kmean {

    public static class KmeansMaper
            extends Mapper<Object, Text, IntWritable, Text >{


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.getConfiguration();


            /* collect existing centers from hadoop file system */
            Path centerPath = new Path("hdfs://localhost:9000/user/KM_center/centers");

            FileSystem fs = centerPath.getFileSystem(context.getConfiguration());
            // FileSystem fs = FileSystem.get(context.getConfiguration()) ;

            List <SiftDescriptor> centerCluster  = new ArrayList();

            BufferedReader cacheReader = new BufferedReader(new InputStreamReader(fs.open(centerPath)));

                try {
                    String line ;
                    while (( line = cacheReader.readLine()) != null) {

                        String[] entries = line.split("\\t");
                        centerCluster.add(new SiftDescriptor(entries[1]));

                    }
                } finally {
                    cacheReader.close();
                }

            //  identify center for each sift
            String siftLine = value.toString();
            SiftDescriptor sift = new SiftDescriptor(siftLine);
            SiftDescriptor center = sift.findNearest(centerCluster);
            int centerKey = center.getIndex();
            context.write(new IntWritable(centerKey) ,value);

        }

    }


    public static class KmeansReducer
            extends Reducer<IntWritable,Text, IntWritable, Text> {

        private Text newCenterText = new Text();
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

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

        int loop = 0;
        // in case infinitive loop
        int MAX_LOOP = 9999;
        boolean isCenterFixed = false;
        int k = Integer.parseInt(args[0]);

        while (loop < MAX_LOOP  && !isCenterFixed ) {

            Job job = getMapReduceJob(args);
            job.waitForCompletion(true);

            loop++;
            Path outputPath = new Path("hdfs://localhost:9000/user/KM_output");
            Path newCenterPath = new Path("hdfs://localhost:9000/user/KM_output/part-r-00000");
            Path centerPath = new Path("hdfs://localhost:9000/user/KM_center/centers");

            FileSystem fs = newCenterPath.getFileSystem(job.getConfiguration());

            List <SiftDescriptor> newCenterCluster = SiftDescriptor.getCenterClusterFromInStream(fs.open(newCenterPath));
            List <SiftDescriptor> oldCenterCluster = SiftDescriptor.getCenterClusterFromInStream(fs.open(centerPath));

            int maxDistance = SiftDescriptor.maxDistance(newCenterCluster, oldCenterCluster, k);
            if(maxDistance < 100) isCenterFixed = true;

            // replace old centre files with latest generated one
            // then delete old output folder
            fs.rename(newCenterPath, centerPath);
            fs.delete(outputPath,true);

        }


//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
