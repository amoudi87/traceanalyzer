package org.amoudi.trace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class LineageComputer {
    private static final String KEY_NAME = "name";
    private static final String KEY_CATEGORY = "cat";
    private static final String KEY_PHASE = "ph";
    private static final String KEY_TIMESTAMP = "ts";
    private static final String KEY_ARGS = "args";
    private static final String KEY_SIZE = "size";
    private static final String CATEGORY_FLUSH = "flush";
    private static final String CATEGORY_MERGE = "merge";
    private static final String PHASE_BEGIN = "B";
    private static final String PHASE_END = "E";
    private static final String DATE_SAMPLE = "2017-10-17-23-08-06-570";
    private static final String SUFFIX_SAMPLE = DATE_SAMPLE + "_" + DATE_SAMPLE + "_b";
    private static final int SUFFIX_LENGTH = SUFFIX_SAMPLE.length();
    private static final Format FORMATTER = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");

    private static void help() {
        System.out.println("This class is used to compute the tree of flushes and merges."
                + " The file passed is expected to only have schedule io events of a single index");
        System.out.println("Expected arguments are:");
        System.out.println("<file name>");
    }

    public static File compute(String input) throws Exception{
        File file= new File(input);
        if (!file.exists()) {
            throw new Exception("File" + file.getAbsolutePath() + " doesn't exist");
        }
        ObjectMapper om = new ObjectMapper();
        String outputFileName = file.getAbsolutePath();
        outputFileName = outputFileName.substring(0, outputFileName.lastIndexOf('.')) + ".lineage.txt";
        System.out.println("Writing output to " + outputFileName);
        File outputFile = new File(outputFileName);
        Map<Long, Integer> time2Num = new HashMap<>();
        int counter = 0;
        Stack<JsonNode> flushStarts = new Stack<>();
        Stack<JsonNode> mergeStarts = new Stack<>();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
             BufferedReader br = new BufferedReader(new FileReader(file))) {
            bw.write("[\n");
            boolean first = true;
            for (String line; (line = br.readLine()) != null;) {
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
                JsonNode property = json.findValue(KEY_NAME);
                if (property == null) {
                    System.out.println(line + " doesn't contain a " + KEY_NAME + " field");
                    help();
                    System.exit(1);
                }
                String fileName = property.asText();
                if (fileName == null || fileName.length() == 0) {
                    System.out.println(line + " doesn't contain a " + KEY_NAME + " field of type String");
                    help();
                    System.exit(1);
                }
                // Get Category
                JsonNode categoryNode = json.findValue(KEY_CATEGORY);
                if (categoryNode == null) {
                    System.out.println(line + " doesn't contain a " + KEY_CATEGORY + " field");
                    help();
                    System.exit(1);
                }
                String categoryString = categoryNode.asText();
                if (categoryString == null || categoryString.length() == 0) {
                    System.out.println(line + " doesn't contain a " + KEY_CATEGORY + " field of type String");
                    help();
                    System.exit(1);
                }

                // Get phase
                JsonNode phaseNode = json.findValue(KEY_PHASE);
                if (phaseNode == null) {
                    System.out.println(line + " doesn't contain a " + KEY_PHASE + " field");
                    help();
                    System.exit(1);
                }
                String phaseString = phaseNode.asText();
                if (phaseString == null || phaseString.length() == 0) {
                    System.out.println(line + " doesn't contain a " + KEY_PHASE + " field of type String");
                    help();
                    System.exit(1);
                }
                if (!(phaseString.equals(PHASE_BEGIN) || phaseString.equals(PHASE_END))) {
                    System.out.println(line + " doesn't contain a begin or end duration trace");
                    continue;
                }

                // format
                // 2017-10-17-23-08-06-570_2017-10-17-23-08-06-570_b
                int lengthOfFileName = fileName.length();
                int beginIndex = lengthOfFileName - SUFFIX_LENGTH;
                String range = fileName.substring(beginIndex, beginIndex + SUFFIX_LENGTH - 2);
                String begin = range.substring(DATE_SAMPLE.length() + 1);
                Date beginDate = parse(begin);
                switch (categoryString) {
                    case CATEGORY_FLUSH:
                        switch (phaseString) {
                            case PHASE_BEGIN:
                                flushStarts.push(json);
                                break;
                            case PHASE_END:
                                // get the start
                                JsonNode startJson = flushStarts.pop();
                                // get start time
                                long startTimestamp = startJson.findValue(KEY_TIMESTAMP).asLong();
                                // get end time
                                long endTimestamp = json.findValue(KEY_TIMESTAMP).asLong();
                                // get size
                                JsonNode args = json.findValue(KEY_ARGS);
                                if (args == null) {
                                    System.out.println(line + " doesn't contain args");
                                    help();
                                    System.exit(1);
                                }
                                JsonNode sizeField = args.findValue(KEY_SIZE);
                                if (sizeField == null) {
                                    break;
                                }
                                long size = sizeField.asLong();
                                long duration = endTimestamp - startTimestamp;
                                // get start date as long
                                time2Num.put(beginDate.getTime(), counter);
                                if (first) {
                                    first = false;
                                } else {
                                    bw.write(",\n");
                                }
                                bw.write("{\"op\":\"flush\", \"id\":\"" + counter + "\", \"duration\":" + duration
                                        + ", \"size\":" + size + "}");

                                counter++;
                                break;
                        }
                        break;
                    case CATEGORY_MERGE:
                        switch (phaseString) {
                            case PHASE_BEGIN:
                                mergeStarts.push(json);
                                break;
                            case PHASE_END:
                                // get the start
                                JsonNode startJson = mergeStarts.pop();
                                // get start time
                                long startTimestamp = startJson.findValue(KEY_TIMESTAMP).asLong();
                                // get end time
                                long endTimestamp = json.findValue(KEY_TIMESTAMP).asLong();
                                // get size
                                JsonNode args = json.findValue(KEY_ARGS);
                                if (args == null) {
                                    System.out.println(line + " doesn't contain args");
                                    help();
                                    System.exit(1);
                                }
                                JsonNode sizeField = args.findValue(KEY_SIZE);
                                if (sizeField == null) {
                                    break;
                                }
                                long size = sizeField.asLong();
                                long duration = endTimestamp - startTimestamp;

                                // get start date as long
                                String end = range.substring(0, DATE_SAMPLE.length());
                                Date endDate = parse(end);
                                if (first) {
                                    first = false;
                                } else {
                                    bw.write(",\n");
                                }
                                bw.write("{\"op\":\"merge\",\"id\":\"" + time2Num.get(beginDate.getTime()) + "-"
                                        + time2Num.get(endDate.getTime()) + "\", \"duration\":" + duration
                                        + ", \"size\":" + size + "}");
                                // get start date as long
                                // get end date as long
                                break;
                        }
                        break;
                    default:
                        System.out.println(line + " doesn't contain a " + KEY_CATEGORY + " field of either "
                                + CATEGORY_FLUSH + " or " + CATEGORY_MERGE);
                        help();
                        System.exit(1);
                }
            }
            bw.write("\n]");
        }
        return outputFile;
    }

    private static Date parse(String date) throws ParseException {
        return (Date) FORMATTER.parseObject(date);
    }
}
