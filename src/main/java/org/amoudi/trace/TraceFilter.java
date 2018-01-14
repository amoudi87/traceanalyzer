package org.amoudi.trace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;

public class TraceFilter {

    private static final String KEY_FILE = "f";
    private static final char EXACT = 'e';
    private static final char CONTAINS = 'c';

    private static void help() {
        System.out.println("This tool is used to extract traces that matches some predicate from a trace file");
        System.out.println("The search predicates are OR combined and currently this only works on String fields");
        System.out.println("Expected arguments are:");
        System.out.println("-f <file name>");
        System.out.println("-c<property> <key> (contains match)");
        System.out.println("-e<property> <key> (exact match)");
    }

    public static File filter(String fileName, Map<String, List<Pair<Character, String>>> predicates) throws Exception {
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
        return extract(input, predicates);
    }

    private static File extract(File file, Map<String, List<Pair<Character, String>>> predicates)
            throws IOException, InterruptedException {
        ObjectMapper om = new ObjectMapper();
        String outputFileName = file.getAbsolutePath();
        outputFileName = outputFileName.substring(0, outputFileName.lastIndexOf('.')) + ".filtered";
        for (Map.Entry<String, List<Pair<Character, String>>> predicate : predicates.entrySet()) {
            outputFileName = outputFileName + "." + predicate.getKey();
        }
        outputFileName = outputFileName + ".json";
        System.out.println("Writing output to " + outputFileName);
        File outputFile = new File(outputFileName);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
             BufferedReader br = new BufferedReader(new FileReader(file))) {
            bw.write("[\n");
            int totalIn = 0;
            int totalOut = 0;
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
                    throw e;
                }
                // We got the json, now we check if it should be included
                for (Map.Entry<String, List<Pair<Character, String>>> predicate : predicates.entrySet()) {
                    JsonNode property = json.findValue(predicate.getKey());
                    if (property != null) {
                        String asString = property.asText();
                        if (asString != null && asString.length() > 0) {
                            for (Pair<Character, String> pair : predicate.getValue()) {
                                switch (pair.getKey()) {
                                    case EXACT:
                                        if (asString.equals(pair.getValue())) {
                                            break;
                                        }
                                        continue;
                                    case CONTAINS:
                                        if (asString.contains(pair.getValue())) {
                                            break;
                                        }
                                        continue;
                                }
                                bw.write(json.toString() + ",\n");
                                totalOut++;
                            }
                        }
                    }
                }
            }
            System.out.println("Total in: " + totalIn + ". Total out: " + totalOut);
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
