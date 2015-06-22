package com.distributKM.sift;


import com.distributKM.KmeanLocal;

import java.io.*;
import java.util.*;


/**
 * Created by lichaochen on 15/5/11.
 */
public class SiftDescriptor {

    double desc[] = new double[128];
    double x;
    double y;

    public void setY(double y) {
        this.y = y;
    }

    public double getY() {
        return y;
    }

    public void setX(double x){
        this.x = x;
    }
    public double getX(){
        return this.x;
    }

    public double[] getDescArray() {
        return desc;
    }

    //只有质心有标号
    int index = 0;
    public int getIndex() {return index;}
    public void setIndex(int _index) {index = _index; }

    public SiftDescriptor() {

    }

    // 把文件行转化为sift descriptor对象
    public SiftDescriptor(String line) {

        String[] items = line.split(" ");
        double[] descArray = this.getDescArray();
        // compatible with both formats
        // 1st - 128 (descriptor)
        // 2nd - 4(feature point)  + 128 (descriptor)

        if(items.length > 128) {
            setX(Double.parseDouble(items[0]));
            setY(Double.parseDouble(items[1]));

            // item 1-4 is feature point info, sift descriptors starting from 5th item
            for (int i = 0; i < descArray.length; i++) {
                descArray[i] = Double.parseDouble(items[i+4]);
            }
        } else {

            for (int i = 0; i < descArray.length; i++) {
                descArray[i] = Double.parseDouble(items[i]);
            }
        }


    }

    // 生成随机的sift descriptor对象
    public static SiftDescriptor getRandomSift() {
        SiftDescriptor sift = new SiftDescriptor();
        for (int i = 0; i < sift.getDescArray().length; i++) {
            sift.getDescArray()[i] = Math.random() * 256;
        }
        return sift;
    }

    // 复制另一个sift descriptor对象
    public void copy(SiftDescriptor another) {
        int i = 0;
        for (double desc : another.getDescArray()) {
            this.getDescArray()[i++] = desc;
        }

    }

    // 序列化
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (double desc : this.getDescArray()) {
            sb.append(desc);
            sb.append(" ");
        }
        return sb.toString();
    }

    // 计算均值
    public SiftDescriptor shrink(int k) {

        for (int i = 0; i < this.getDescArray().length; i++) {
            this.getDescArray()[i] = this.getDescArray()[i] / k;
        }
        return this;

    }

    // 加法操作，对每一个维度进行加操作
    public SiftDescriptor add(SiftDescriptor another) {

        for (int i = 0; i < another.getDescArray().length; i++) {
            this.getDescArray()[i] += another.getDescArray()[i];
        }
        return this;
    }


    // 计算聚类中心
    public static SiftDescriptor getCenterDescriptor(List<SiftDescriptor> cluster) {

        SiftDescriptor center = new SiftDescriptor();
        int clusterSize = cluster.size();
        for (int i = 0; i < clusterSize; i++) {
            center.add(cluster.get(i));
        }
        center.shrink(clusterSize);
        return center;
    }


    // 计算两个点之间的距离，欧式距离
    public double getDistance(SiftDescriptor another) {

        double sum = 0.0;
        for (int i = 0; i < this.getDescArray().length; i++) {
            sum += (this.getDescArray()[i] - another.getDescArray()[i]) * (this.getDescArray()[i] - another.getDescArray()[i]);
        }

        return sum;
    }

    // 寻找最相近的点
    public SiftDescriptor findNearest(List<SiftDescriptor> list) {

        double minDistance = Double.MAX_VALUE;
        SiftDescriptor nearest = this;
        for (SiftDescriptor another : list) {
            double distance = this.getDistance(another);
            if (distance < minDistance) {
                minDistance = distance;
                nearest = another;
            }
        }
        return nearest;

    }

    // 判断质心之间的移动是否已经满足阈值
    public static double maxDistance(List<SiftDescriptor> cluster1,
                                      List<SiftDescriptor> cluster2,
                                      int centroidNumber) {
        //use cluster1 as driven side , 2 list should be same sized
        Map<Integer, SiftDescriptor> newCentroids = new HashMap<Integer, SiftDescriptor>();
        Map<Integer, SiftDescriptor> oldCentroids = new HashMap<Integer, SiftDescriptor>();
        double maxDistance = 0.0;

        for (SiftDescriptor centroid : cluster1){
            newCentroids.put(centroid.getIndex(), centroid);
        }
        for (SiftDescriptor centroid : cluster2){
            oldCentroids.put(centroid.getIndex(), centroid);
        }

        for (int i = 0; i < centroidNumber + 1; i++) {
            if(newCentroids.get(i) != null) {
                double distanceOfEachCenter = newCentroids.get(i).getDistance(oldCentroids.get(i));
                if(maxDistance < distanceOfEachCenter) maxDistance = distanceOfEachCenter;
            }
        }

        return maxDistance;
    }

    // 根据质心文件生成sift descriptor
    public static List<SiftDescriptor> getCenterClusterFromInStream(InputStream is) throws IOException {
        List<SiftDescriptor> cluster = new ArrayList<SiftDescriptor>();


        BufferedReader cacheReader = new BufferedReader(new InputStreamReader(is));
        try {
            String line;
            while ((line = cacheReader.readLine()) != null) {

                String[] entries = line.split("\\t");
                SiftDescriptor newSift = new SiftDescriptor(entries[1]);
                newSift.setIndex(Integer.parseInt(entries[0]));
                cluster.add(newSift);
            }
        } finally {
            cacheReader.close();
        }

        return cluster;
    }


    /*
        统计每个sift特征在每个词袋中的数量
        每个region的视图 1:
        每张图片的视图
     */
    public static String getHistogram(String fileIndex,
                                      List<SiftDescriptor> siftCluster,
                                      List<SiftDescriptor> centroidCluster,
                                      FileWriter totalWriter, String regionFolder) throws Exception{

        String result = "";
        String regionD = regionFolder;


        // 区域矩阵
        File regionF = new File(regionD + fileIndex + ".mask");
        int[][] regionMatrix = getRegionMartixFromInStream(new FileInputStream(regionF));
        int maxRegion = getMaxRegion(regionMatrix);
        // 质心数目
        int centroidNumber = centroidCluster.size();

        int[][] resultMatrix = new int[maxRegion + 2][centroidNumber];

        for (SiftDescriptor sift : siftCluster) {
            SiftDescriptor centroid = sift.findNearest(centroidCluster);
            int region = sift.getRegion(regionMatrix);
            int cenKey = centroidCluster.indexOf(centroid);
            resultMatrix[region][cenKey] ++;
            resultMatrix[maxRegion + 1][cenKey] ++;
        }


        for (int i = 0; i < maxRegion + 1; i ++){
            result += i;
            result += " ";
            for (int j = 0; j < centroidNumber; j ++) {
                result += String.valueOf(resultMatrix[i][j]);
                result += " ";

            }
            result += "\n";
        }
        String total = fileIndex + " ";
        for(int k = 0; k < centroidNumber; k ++){
            total += String.valueOf(resultMatrix[maxRegion + 1][k]);
            total += " ";
        }
        total += "\n";
        totalWriter.write(total);

        return result;
    }

    /*
    把region文件转化为矩阵
     */
    public static int[][] getRegionMartixFromInStream(InputStream is) throws IOException {
        List<String[]> fileContent = new ArrayList<String[]>();
        int rowNumber = 0;
        int colNumber = 0;

        BufferedReader cacheReader = new BufferedReader(new InputStreamReader(is));
        try {
            String line;
            while ((line = cacheReader.readLine()) != null) {
                String[] rowContent = line.split(" ");
                fileContent.add(rowContent);
                rowNumber ++;
            }
        } finally {
            cacheReader.close();
        }

        colNumber = fileContent.get(0).length;

        int[][] matrix = new int[rowNumber][colNumber];

        for(int row = 0; row < rowNumber - 1; row ++){
            for(int col = 0; col < colNumber - 1; col ++ ){
                matrix[row][col] = Integer.parseInt(fileContent.get(row)[col]);
            }
        }

        return matrix;
    }

    //获取sift的区域编号
    public int getRegion(int[][] regionMatrix) {
        int x = (int)Math.round(this.getX());
        int y = (int)Math.round(this.getY());

        return regionMatrix[y][x];
    }

    // 获取最大的region数
    private static int getMaxRegion(int[][] regionMatrix){
        int rowNumber = regionMatrix.length;
        int colNumber = regionMatrix[0].length;
        int maxRegion = 0;

        for (int i = 0; i < rowNumber; i ++){
            for (int j = 0; j < colNumber; j ++){
                if (regionMatrix[i][j] > maxRegion) {
                    maxRegion = regionMatrix[i][j];
                }
            }
        }

        return maxRegion;
    }

    public static int getRegionsCount(int[][] regionMatrix){
        //region seq starting from zero - 0, so total count is max seq + 1
        return getMaxRegion(regionMatrix)+1;
    }

    // 根据input获取sift cluster
    public static List<SiftDescriptor> getsiftClusterFromInStream(InputStream is) throws IOException {
        List<SiftDescriptor> cluster = new ArrayList<SiftDescriptor>();


        BufferedReader cacheReader = new BufferedReader(new InputStreamReader(is));
        try {
            String line;
            while ((line = cacheReader.readLine()) != null) {
                cluster.add(new SiftDescriptor(line));
            }
        } finally {
            cacheReader.close();
        }

        return cluster;
    }

    // 使用随机质心补充缺失的质心（参考遗传算法，随机出新的种群）
    public static void fullfillCenter(File centerFile, List<SiftDescriptor> centroids, int centerNumber) throws IOException{
        FileWriter fileWriter = new FileWriter(centerFile);
        Map<Integer, SiftDescriptor> centroidMap = new HashMap<Integer, SiftDescriptor>();

        for(SiftDescriptor centroid : centroids){
            centroidMap.put(centroid.getIndex(), centroid);
        }

        for(int i = 0; i < centerNumber; i ++){
            StringBuilder sb = new StringBuilder();
            sb.append(i);
            sb.append("\t");
            if(centroidMap.get(i) != null){
                sb.append(centroidMap.get(i).toString() + "\n");
            }else{
                SiftDescriptor randomSift = SiftDescriptor.getRandomSift();
                sb.append(randomSift.toString() + "\n");
            }

            fileWriter.write(sb.toString());
        }

        fileWriter.close();
    }

    public static void fullfillCenter(OutputStream output, List<SiftDescriptor> centroids, int centerNumber) throws IOException{

        Map<Integer, SiftDescriptor> centroidMap = new HashMap<Integer, SiftDescriptor>();

        for(SiftDescriptor centroid : centroids){
            centroidMap.put(centroid.getIndex(), centroid);
        }

        for(int i = 0; i < centerNumber ; i ++){
            StringBuilder sb = new StringBuilder();
            sb.append(i);
            sb.append("\t");
            if(centroidMap.get(i) != null){
                sb.append(centroidMap.get(i).toString() + "\n");
            }else{
                SiftDescriptor randomSift = SiftDescriptor.getRandomSift();
                sb.append(randomSift.toString() + "\n");
            }

            output.write(sb.toString().getBytes());
        }

        output.close();
    }



    public static void main(String args[]) throws Exception {

//        String line1 = "238 60 0 0 10 5 9 49 50 12 6 6 3 0 1 2 0 1 7 4 1 0 1 1 0 2 1 0 0 0 1 1 238 85 0 0 9 2 0 0 45 6 1 4 4 9 8 2 8 6 1 1 1 1 6 3 0 3 0 0 0 0 0 0 238 95 0 0 2 0 1 1 24 5 1 8 10 7 3 2 16 3 0 1 2 1 0 9 1 0 0 0 0 0 0 0 238 73 1 0 0 0 0 1 6 4 5 2 3 7 4 1 0 0 3 1 0 1 1 2 0 0 0 0 0 0 0 0 ";
//        String line2 = "0 0 0 0 0 0 0 0 7 1 1 8 0 0 0 1 1 0 1 83 25 0 0 0 0 0 0 21 12 0 0 0 16 1 0 14 9 1 9 23 125 47 7 29 9 1 4 18 18 19 9 125 123 5 5 4 0 0 0 112 57 0 0 0 98 1 2 13 9 61 63 125 115 12 5 27 11 3 32 111 40 5 1 79 73 13 44 45 1 0 1 125 87 0 0 0 125 83 13 12 18 56 20 80 125 19 3 36 20 0 1 31 93 3 2 27 10 0 2 43 10 2 16 109 66 40 3 6 ";
//
//        SiftDescriptor sift1 = new SiftDescriptor(line1);
//        SiftDescriptor sift2 = new SiftDescriptor(line2);
//        SiftDescriptor sift3 = new SiftDescriptor();
//
//        System.out.println("distance: " + sift1.getDistance(sift2));
//        sift3.copy(sift1);
//        System.out.println(sift3);
//        System.out.println(sift1.add(sift2));
//        System.out.println(sift1.shrink(2));
//
//        List cluster = new ArrayList();
//        sift1 = new SiftDescriptor(line1);
//        cluster.add(sift1);
//        cluster.add(sift2);
//
//
//        SiftDescriptor sift4 = SiftDescriptor.getCenterDescriptor(cluster);
//        List cluster2 = new ArrayList();
//
//
//        cluster2.add(sift1);
//        cluster2.add(sift4);
//
//        System.out.println(SiftDescriptor.getCenterDescriptor(cluster));
//
//        System.out.println(SiftDescriptor.maxDistance(cluster, cluster2, 2));
//
//
//        for (int i = 0; i < 50; i++) {
//            System.out.println(SiftDescriptor.getRandomSift());
//        }



//        File test = new File("KM_input/test.txt");
////        PrintWriter pw =new PrintWriter(new OutputStreamWriter(new BufferedOutputStream( new FileOutputStream(test)) )) ;
//        BufferedWriter bw =new BufferedWriter(new OutputStreamWriter(( new FileOutputStream(test)) )) ;
//        bw.write("vincent is great !!");
//        bw.newLine();
//        bw.write("Thanks.");
//        bw.close();
////        pw.flush();
//
//        SiftDescriptor.fullfillCenter(new FileOutputStream(test),new ArrayList<SiftDescriptor>(),100);




//        File totalFile = new File("KM_input/");
//
//        File[] files = totalFile.listFiles();
//
//        System.out.println(files);

        System.out.print("hello");
    }
}
