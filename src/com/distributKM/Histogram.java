package com.distributKM;

import com.distributKM.sift.SiftDescriptor;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by pishilong on 15/5/15.
 */
public class Histogram {

    public static void main(String[] args) throws Exception {
        File centerF = new File("KM_center/centers");
        File siftD = new File("KM_input");
        String histogramDic = "histogram/";
        File totalFile = new File("histogram/total.his");
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
            String histogramResult = SiftDescriptor.getHistogram(fileIndex, siftCluster, centerCluster, totalWriter);
            hisWriter.write(histogramResult);
            hisWriter.close();
        }
        totalWriter.close();

    }
}
