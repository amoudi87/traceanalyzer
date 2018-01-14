package org.amoudi.trace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ThreadExtractor {

    private static final String KEY_THREADS = "t";
    private static final String THREAD_FIELD_NAME = "tid";
    private static final String PROCESS_FIELD_NAME = "pid";

    private static void help() {
        System.out.println("This tool is used to extract thread specific traces from a trace file");
        System.out.println("Expected arguments are:");
        System.out.println("-t <[<pid>:]<tid>,[<pid>:]<tid>,...,[<pid>:]<tid>>");
        System.out.println("-f <file name>");
    }

    public static File extract(String fileName, List<Pair<String,Long>> threads, String outputDir) throws Exception {

        if (threads == null) {
            System.out.println("Incorrect use. Missing argument -" + KEY_THREADS);
            help();
            System.exit(1);
        }
        if (fileName == null) {
            System.out.println("Incorrect use. Missing argument -" + fileName);
            help();
            System.exit(1);
        }
        File input = new File(fileName);
        if (!input.exists()) {
            System.out.println("File" + input.getAbsolutePath() + " doesn't exist");
            System.exit(1);
        }
        return extract(input, threads, outputDir);
    }

    private static File extract(File file, List<Pair<String, Long>> processesAndThreads, String outputDir)
            throws IOException, InterruptedException {
        ObjectMapper om = new ObjectMapper();
        String outputFileName = file.getParentFile().getAbsolutePath();
        outputFileName = outputFileName + File.separator + outputDir;
        Path outputDirPath = Paths.get(outputFileName);
        if(!Files.exists(outputDirPath)){
            FileUtils.forceMkdir(outputDirPath.toFile());
        }
        outputFileName = outputFileName + File.separator + file.getName() + ".filtered";
        for (Pair<String, Long> pidtid : processesAndThreads) {
            String pid = pidtid.getLeft();
            Long tid = pidtid.getRight();
            if (pid != null) {
                outputFileName = outputFileName + "." + pid + '.' + tid;
            } else {
                outputFileName = outputFileName + "." + tid;
            }

        }
        outputFileName = outputFileName + ".json";
        System.out.println("Writing output to " + outputFileName);
        File outputFile = new File(outputFileName);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
             BufferedReader br = new BufferedReader(new FileReader(file))) {
            bw.write("[\n");
            int totalIn = 0;
            int[] perThread = new int[processesAndThreads.size()];
            for (String line; (line = br.readLine()) != null;) {
                totalIn++;
                line = line.trim();
                if (!line.startsWith("{")) {
                    System.out.println(line + " doesn't contain a json object");
                    continue;
                }
                line = line.substring(0, line.lastIndexOf('}') + 1);
                JsonNode json;
                try {
                    json = om.readTree(line);
                } catch (Exception e) {
                    System.err.println("Failed parsing: " + line);
                    continue;
                }
                JsonNode threadId = json.findValue(THREAD_FIELD_NAME);
                if (threadId == null) {
                    System.out.println(line + " doesn't contain a " + THREAD_FIELD_NAME + " object");
                    continue;
                }
                JsonNode processId = json.findValue(PROCESS_FIELD_NAME);
                for (int t = 0; t < processesAndThreads.size(); t++) {
                    if (processesAndThreads.get(t).getRight().equals(threadId.longValue())
                            && (processesAndThreads.get(t).getLeft() == null
                            || processesAndThreads.get(t).getLeft().equals(processId.asText()))) {
                        perThread[t]++;
                        bw.write(json.toString() + ",\n");
                        break;
                    }
                }
            }
            int totalOut = 0;
            for (int i = 0; i < perThread.length; i++) {
                totalOut += perThread[i];
            }
            System.out.println("Total in: " + totalIn + ". Total out: " + totalOut);
            for (int i = 0; i < perThread.length; i++) {
                System.out.println("Thread " + processesAndThreads.get(i) + ": " + perThread[i]);
            }
        }
        return outputFile;
    }

    private static String getArgumentKey(String key) {
        if (key.charAt(0) != '-' || key.length() <= 1) {
            System.out.println("Incorrect use. Malformed argument: " + key);
            help();
            System.exit(1);
        }
        return key.substring(1);
    }
}
