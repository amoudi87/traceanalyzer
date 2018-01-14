package org.amoudi.trace;

import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Driver {
    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            System.err.println("Must pass list of files argument");
            System.exit(1);
        }
        // for every file
        // -- for each thread
        // ----- filter out
        // ----- compute time breakdown
        // -- find every index
        // -- compute lineage of every index
        // -- compute tree height of every index
        // -- produce a report
        for(String fileName: args){
            // -- find ingestion and storage threads
            Map<String,List<String>> predicates = new HashMap<>();
            String key = "name";
            List<String> values = new ArrayList<>();
            values.add("Write-Network-Ingestion-To-Store");
            predicates.put(key,values);
            List<Pair<String, Long>> ingestionThreads = ThreadFinder.find(fileName, predicates);
            values.clear();
            values.add("Ingestion-Store");
            List<Pair<String, Long>> storageThreads = ThreadFinder.find(fileName, predicates);
            System.out.println("Found ingestion threads are: " + ingestionThreads +" and storage threads are: " + storageThreads);
            List<File> filteredIngestionFiles = new ArrayList<>();
            List<File> filteredStorageFiles = new ArrayList<>();
            for (Pair<String,Long> thread: ingestionThreads){
                filteredIngestionFiles.add(ThreadExtractor.extract(fileName, Collections.singletonList(thread), "analysis"+ File.separator+"ingestion"));
            }
            for (Pair<String,Long> thread: storageThreads){
                filteredStorageFiles.add(ThreadExtractor.extract(fileName, Collections.singletonList(thread), "analysis"+ File.separator+"storage"));
            }

            // TODO: Find indexes, compute their lineage and height, then add to the report

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(getOutputFile(fileName)))){
                bw.write("Analysis report");
                bw.newLine();
                bw.write("Ingestion threads found: ");
                bw.write(ingestionThreads.toString());
                bw.newLine();
                bw.write("Storage threads found: ");
                bw.write(storageThreads.toString());
                bw.newLine();
                bw.newLine();
                bw.write("=======================================");
                bw.write("Breakdown of ingestion threads: ");
                bw.newLine();
                for(int i=0;i<ingestionThreads.size();i++){
                    bw.write("Process: ");
                    bw.write(ingestionThreads.get(i).getKey());
                    bw.write(" Thread: ");
                    bw.write(Long.toString(ingestionThreads.get(i).getValue()));
                    bw.newLine();
                    TimeBreaker.breakdown(filteredIngestionFiles.get(i),bw);
                    bw.newLine();
                }
                bw.write("=======================================");
                bw.write("Breakdown of storage threads: ");
                bw.newLine();
                for(int i=0;i<storageThreads.size();i++){
                    bw.write("Process: ");
                    bw.write(storageThreads.get(i).getKey());
                    bw.write(" Thread: ");
                    bw.write(Long.toString(storageThreads.get(i).getValue()));
                    bw.newLine();
                    TimeBreaker.breakdown(filteredStorageFiles.get(i),bw);
                    bw.newLine();
                }
            }
        }
    }

    private static String getOutputFile(String fileName) throws IOException {
        File inputFile = new File(fileName);
        String outputFileName = inputFile.getParentFile().getAbsolutePath();
        outputFileName = outputFileName + File.separator + "analysis";
        Path outputDirPath = Paths.get(outputFileName);
        if(!Files.exists(outputDirPath)){
            Files.createDirectory(outputDirPath);
        }
        return outputFileName + File.separator + "report.txt";
    }
}
