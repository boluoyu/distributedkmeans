package com.distributKM.sift;



import java.io.*;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;

/**
 * Created by lichaochen on 15/5/11.
 */
public class SiftDescriptor {

    int desc [] = new int[128];

    public int[] getDescArray(){
        return desc;
    }

    public SiftDescriptor(){

    }

    public SiftDescriptor (String line){

        String [] items= line.split(" ");
        int [] descArray = this.getDescArray();

        for (int i =0 ; i<items.length; i++){
            descArray[i] = Integer.parseInt(items[i]);
        }

//        for (int i : descArray){
//            System.out.println(i);
//        }
//        System.out.println(this.getDescArray().length);

    }
    public  static SiftDescriptor getRandomSift(){
        SiftDescriptor sift = new SiftDescriptor();
        for( int i = 0;i < sift.getDescArray().length; i++){
            sift.getDescArray()[i]= (int)(Math.random() * 256);
        }
        return sift;
    }

    public void copy(SiftDescriptor another){

        int i =0;
    for( int desc : another.getDescArray()){
        this.getDescArray()[i++] = desc;
    }

}

    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (int desc : this.getDescArray()){
            sb.append(desc);
            sb.append(" ");
        }
        return sb.toString();
    }

    public SiftDescriptor shrink(int k){

        for (int i =0; i<this.getDescArray().length; i++){
            this.getDescArray()[i]=this.getDescArray()[i]/k;
        }
        return this;

    }

    public SiftDescriptor add( SiftDescriptor another){

        for (int i =0; i<another.getDescArray().length; i++){
            this.getDescArray()[i]+=another.getDescArray()[i];
        }
        return this;
    }


    public static SiftDescriptor getCenterDescriptor (List <SiftDescriptor> cluster){

        SiftDescriptor center = new SiftDescriptor();
        int clusterSize = cluster.size();
        for(int i = 0; i < clusterSize; i++){
            center.add(cluster.get(i));
        }
        center.shrink(clusterSize);
        return center;
    }



    public int getDistance(SiftDescriptor another){

        int sum =0;
        for (int i = 0; i < this.getDescArray().length; i++){
            sum += (this.getDescArray()[i] - another.getDescArray()[i]) * (this.getDescArray()[i] - another.getDescArray()[i]) ;
        }

        return sum;
    }

    public SiftDescriptor findMostSimilar(List<SiftDescriptor> list){

        int distance = Integer.MAX_VALUE;
        SiftDescriptor mostSimilar = this;
        for (SiftDescriptor another: list){
           if (this.getDistance(another) < distance) {
                distance = this.getDistance(another);
               mostSimilar =another;
            }
        }
        return mostSimilar;

    }

    public static boolean isClusterDistanceLessThenThreshold(List<SiftDescriptor> cluster1, List<SiftDescriptor> cluster2, double threshold){
        //use cluster1 as driven side , 2 list should be same sized

        int size1= cluster1.size();

       for (int i =0; i < size1; i++){
           int distanceOfEachCenter =  cluster1.get(i).getDistance(cluster2.get(i));
           if (distanceOfEachCenter > threshold) {
               // as long as distance of one pair of center is longer than threshold, return false immediately
               return false;
           }
       }

        return true;
    }

    public static List<SiftDescriptor> getCenterClusterFromInStream (InputStream is) throws IOException {
        List<SiftDescriptor> cluster = new ArrayList<SiftDescriptor>();


        BufferedReader cacheReader = new BufferedReader(new InputStreamReader(is));
        try {
            String line ;
            while (( line = cacheReader.readLine()) != null) {

                String[] entries = line.split("\\t");
                cluster.add(new SiftDescriptor(entries[1]));
            }
        }
        finally {
            cacheReader.close();
        }

        return cluster;
    }

    public static String getHistogram(List<SiftDescriptor> siftCluster, List<SiftDescriptor> centroidCluster) {
        String histogram = new String();
        String result = "";
        HashMap<Integer, Integer> resultMap = new HashMap<Integer, Integer>();

        for (SiftDescriptor sift : siftCluster) {
            SiftDescriptor centroid = sift.findMostSimilar(centroidCluster);
            int cenKey = centroidCluster.indexOf(centroid);
            int value = 1;
            if(resultMap.get(cenKey) != null) {
                value = resultMap.get(cenKey) + 1;
            }
            resultMap.put(cenKey, value);
        }

        Object[] key =  resultMap.keySet().toArray();
        java.util.Arrays.sort(key);

        for (int i = 0; i < key.length; i++)   {
            result += resultMap.get(key[i]).toString();
            result += " ";
        }

        return result;
    }

    public static List<SiftDescriptor> getsiftClusterFromInStream (InputStream is) throws IOException {
        List<SiftDescriptor> cluster = new ArrayList<SiftDescriptor>();


        BufferedReader cacheReader = new BufferedReader(new InputStreamReader(is));
        try {
            String line ;
            while (( line = cacheReader.readLine()) != null) {
                cluster.add(new SiftDescriptor(line));
            }
        }
        finally {
            cacheReader.close();
        }

        return cluster;
    }

    public static void main(String args []){

        String line1 = "238 60 0 0 10 5 9 49 50 12 6 6 3 0 1 2 0 1 7 4 1 0 1 1 0 2 1 0 0 0 1 1 238 85 0 0 9 2 0 0 45 6 1 4 4 9 8 2 8 6 1 1 1 1 6 3 0 3 0 0 0 0 0 0 238 95 0 0 2 0 1 1 24 5 1 8 10 7 3 2 16 3 0 1 2 1 0 9 1 0 0 0 0 0 0 0 238 73 1 0 0 0 0 1 6 4 5 2 3 7 4 1 0 0 3 1 0 1 1 2 0 0 0 0 0 0 0 0 ";
        String line2 = "0 0 0 0 0 0 0 0 7 1 1 8 0 0 0 1 1 0 1 83 25 0 0 0 0 0 0 21 12 0 0 0 16 1 0 14 9 1 9 23 125 47 7 29 9 1 4 18 18 19 9 125 123 5 5 4 0 0 0 112 57 0 0 0 98 1 2 13 9 61 63 125 115 12 5 27 11 3 32 111 40 5 1 79 73 13 44 45 1 0 1 125 87 0 0 0 125 83 13 12 18 56 20 80 125 19 3 36 20 0 1 31 93 3 2 27 10 0 2 43 10 2 16 109 66 40 3 6 ";

        SiftDescriptor sift1 = new SiftDescriptor(line1);
        SiftDescriptor sift2 = new SiftDescriptor(line2);
        SiftDescriptor sift3  = new SiftDescriptor();

        System.out.println("distance: " + sift1.getDistance(sift2));
        sift3.copy(sift1);
        System.out.println(sift3);
        System.out.println(sift1.add(sift2));
        System.out.println(sift1.shrink(2));

        List cluster = new ArrayList();
        sift1 = new SiftDescriptor(line1);
        cluster.add(sift1);
        cluster.add(sift2);



        SiftDescriptor sift4 = SiftDescriptor.getCenterDescriptor(cluster);
        List cluster2 = new ArrayList();


        cluster2.add(sift1);
        cluster2.add(sift4);

        System.out.println(SiftDescriptor.getCenterDescriptor(cluster));

        System.out.println(SiftDescriptor.isClusterDistanceLessThenThreshold(cluster,cluster2, 100));



        for (int i =0; i<50; i++){
            System.out.println(   SiftDescriptor.getRandomSift());
        }
    }
}
