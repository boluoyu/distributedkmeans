package com.distributKM;

import com.distributKM.sift.SiftDescriptor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

/**
 * Created by pishilong on 15/5/15.
 */
public class HistogramLocal {


    public static void main(String[] args) throws Exception {

        if(args.length<4){
            System.err.println("Usage: HistogramLocal <input_folder> <center_file> <mask_folder> <histogram_folder>");
            System.err.println("Example: HistogramLocal  KM_input/ KM_center/centers KM_mask/ KM_histogram/ ");
            System.exit(2);
        }


        String inputDic = args[0];
        String centerLocation = args[1];
        String maskLocation = args[2];
        String histogramDic = args[3];

        File centerF = new File(centerLocation);
        File siftD = new File(inputDic);


        File totalFile = new File(histogramDic+"total.his");
        if(totalFile.exists()) totalFile.delete();
        totalFile.createNewFile();
        FileWriter totalWriter = new FileWriter(totalFile, true);

        File[] files = siftD.listFiles();
        LinkedList<File> filelist = new LinkedList<File>();
        for (int i = 0; i < files.length; i ++){
            String fileName = files[i].getName();
            if (fileName.endsWith("sift")) {
                filelist.add(files[i]);
            }
        }

        Collections.sort(filelist, new Comparator<File>(){
            @Override
            public int compare(File o1, File o2) {
                int file1Name = Integer.parseInt(o1.getName().split("\\.")[0]);
                int file2Name = Integer.parseInt(o2.getName().split("\\.")[0]);
                if(file1Name < file2Name)
                    return -1;
                if(file1Name > file2Name)
                    return 1;
                return 0;
            }
        });

        List<SiftDescriptor> centerCluster = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream(centerF));
        for (File file : filelist){
            String fileIndex = file.getName().split("\\.")[0];
            File hisFile = new File(histogramDic + fileIndex + ".his");
            if(!hisFile.exists()) hisFile.createNewFile();
            FileWriter hisWriter = new FileWriter(hisFile);
            List<SiftDescriptor> siftCluster = SiftDescriptor.getsiftClusterFromInStream(new FileInputStream(file));
            String histogramResult = SiftDescriptor.getHistogram(fileIndex, siftCluster, centerCluster, totalWriter, maskLocation);
            hisWriter.write(histogramResult);
            hisWriter.close();
        }
        totalWriter.close();

    }
}
