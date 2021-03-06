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

/**
 * Created by lichaochen on 15/5/14.
 */
public class KmeanLocal {
    public static List<SiftDescriptor> centerCluster;

    public static class KmeansMaper
            extends Mapper<Object, Text, IntWritable, Text >{

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            centerCluster = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream( new File("KM_center/centers")));
        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            /* find corresponding center for given sift descriptor */

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

            // find new Center for sift cluster
            SiftDescriptor newCenter = new SiftDescriptor();
            int loop = 0;
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
        if (otherArgs.length < 3) {
            System.err.println("Usage: Kmean <clusterNum> <in> [<in>...] <out>");
            System.exit(2);
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        return job;
    }

    public static void initCentroids(int clusterNum) throws Exception{
        File centerF = new File("KM_center/centers");
        if(!centerF.exists()) {
            centerF.createNewFile();
        }

        FileWriter centerFWrite = new FileWriter(centerF);

        for(int i = 0; i < clusterNum; i++){
            SiftDescriptor randomSift = SiftDescriptor.getRandomSift();
            StringBuilder sb = new StringBuilder();
            sb.append(i);
            sb.append("\t");
            sb.append(randomSift.toString() + "\n");
            centerFWrite.write(sb.toString());
        }
        centerFWrite.close();
    }


    public static void initCentroids(int clusterNum, OutputStream centerOutStream) throws Exception{

        BufferedOutputStream  bs = new BufferedOutputStream(centerOutStream);
        for(int i = 0; i < clusterNum; i++){
            SiftDescriptor randomSift = SiftDescriptor.getRandomSift();
            StringBuilder sb = new StringBuilder();
            sb.append(i);
            sb.append("\t");
            sb.append(randomSift.toString() + "\n");
            bs.write(sb.toString().getBytes());

        }
        bs.close();
    }

    public static void main(String[] args) throws Exception {


        int MAX_LOOP = 1000;
        // int MAX_LOOP = 2;
        boolean isCenterFixed = false;
        int loop = 0;
        int k = Integer.parseInt(args[0]);

        File outputF = new File("KM_output/part-r-00000");
        File centerF = new File("KM_center/centers");

        initCentroids(k,new FileOutputStream(centerF));

        while(loop<MAX_LOOP && !isCenterFixed ){
            Job job = getMapReduceJob(args);

            job.waitForCompletion(true);


            List <SiftDescriptor> newCenter = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream(outputF));
            List <SiftDescriptor> oldCenter = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream(centerF));;

            double maxDistance = SiftDescriptor.maxDistance(newCenter, oldCenter, k);
            isCenterFixed =  maxDistance < 10;

            outputF.renameTo(centerF);
            System.out.println("again!!!----" + loop + " distance: " + maxDistance);
            new File("KM_output/._SUCCESS.crc").delete();
            new File("KM_output/.part-r-00000.crc").delete();
            new File("KM_output/_SUCCESS").delete();
            new File("KM_output").delete();

            //填充缺失的质心
            SiftDescriptor.fullfillCenter(new FileOutputStream(centerF), newCenter, k);
            loop ++;
        }

    }
}
