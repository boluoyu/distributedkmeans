package com.distributKM;

import com.distributKM.sift.SiftDescriptor;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.io.FileWriter;
import java.util.LinkedList;

/**
 * Created by pishilong on 15/5/15.
 */
public class Histogram {

    public static void main(String[] args) throws Exception {
        File centerF = new File("KM_center/centers");
        File siftD = new File("KM_input");
        File histogram = new File("histogram");
        if (!histogram.exists()) {
            histogram.createNewFile();
        }
        FileWriter hisWriter = new FileWriter(histogram);

        File[] files = siftD.listFiles();
        LinkedList<File> filelist = new LinkedList<File>();
        for (int i = 0; i < files.length; i ++){
            String fileName = files[i].getName();
            if (fileName.endsWith("sift")) {
                filelist.add(files[i]);
            }
        }

        List<SiftDescriptor> centerCluster = SiftDescriptor.getCenterClusterFromInStream(new FileInputStream(centerF));
        for (File file : filelist){
            String fileIndex = file.getName().split("\\.")[0];
            List<SiftDescriptor> siftCluster = SiftDescriptor.getsiftClusterFromInStream(new FileInputStream(file));
            String histogramResult = SiftDescriptor.getHistogram(fileIndex, siftCluster, centerCluster);
            String content = "image " + fileIndex + "'s histogram: \n" + histogramResult + "\n";
            hisWriter.write(content);
        }
        hisWriter.close();
    }
}
