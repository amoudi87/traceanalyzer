package org.amoudi.trace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;

public class TimeBreaker {

    private static final String KEY_NAME = "name";
    private static final String KEY_ARGS = "args";
    private static final String KEY_PHASE = "ph";
    private static final String KEY_TIMESTAMP = "ts";
    private static final String KEY_COUNT = "count";
    private static final String KEY_AVG_DURATION_NANO = "avg-duration-ns";
    private static final String PHASE_INSTANT = "i";
    private static final String PHASE_BEGIN = "B";
    private static final String PHASE_END = "E";

    private static void help() {
        System.out.println("This tool is used to produce time breakdown for a single thread trace file");
        System.out.println("Expected arguments are:");
        System.out.println("<file name>");
    }

    public static void breakdown(String fileName, Writer bw) throws Exception {
        File input = new File(fileName);
        if (!input.exists()) {
            System.out.println("File" + input.getAbsolutePath() + " doesn't exist");
            System.exit(1);
        }
        breakdown(input, bw);
    }

    public static void breakdown(File file, Writer bw) throws IOException, InterruptedException {
        long globalStart = Long.MAX_VALUE;
        long globalEnd = Long.MIN_VALUE;

        ObjectMapper om = new ObjectMapper();
        // Sum,Min,Max,Count
        HashMap<String, long[]> name2Duration = new HashMap<>();
        HashMap<String, Long> name2Instant = new HashMap<>();
        Stack<Pair<String, JsonNode>> starts = new Stack<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            for (String line; (line = br.readLine()) != null; ) {
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
                long timestamp = json.findValue(KEY_TIMESTAMP).asLong();
                globalStart = Long.min(globalStart, timestamp);
                globalEnd = Long.max(globalEnd, timestamp);
                // Get event phase
                String phase = json.findValue(KEY_PHASE).asText();
                switch (phase) {
                    case PHASE_BEGIN:
                        // Get name
                        String name = json.findValue(KEY_NAME).asText();
                        starts.push(Pair.of(name, json));
                        break;
                    case PHASE_END:
                        if (starts.isEmpty()) {
                            System.err.println("End event: " + json + " had no start event");
                            break;
                        }
                        Pair<String, JsonNode> startEvent = starts.pop();
                        name = startEvent.getKey();
                        long start = startEvent.getRight().findValue(KEY_TIMESTAMP).asLong();
                        long end = json.findValue(KEY_TIMESTAMP).asLong();
                        long[] nameDuration =
                                name2Duration.getOrDefault(name, new long[]{0L, Long.MAX_VALUE, Long.MIN_VALUE, 0L});
                        long thisDuration = end - start;
                        nameDuration[0] = nameDuration[0] + thisDuration;
                        nameDuration[1] = Long.min(nameDuration[1], thisDuration);
                        nameDuration[2] = Long.max(nameDuration[2], thisDuration);
                        nameDuration[3]++;
                        name2Duration.put(name, nameDuration);
                        break;
                    case PHASE_INSTANT:
                        // For instant events, we will only look at args
                        // In args, we look for count and avg-duration-ns
                        name = json.findValue(KEY_NAME).asText();
                        JsonNode args = json.findValue(KEY_ARGS);
                        if (args == null) {
                            break;
                        }
                        JsonNode countField = args.findValue(KEY_COUNT);
                        if (countField == null) {
                            break;
                        }
                        JsonNode avgDurationField = args.findValue(KEY_AVG_DURATION_NANO);
                        if (avgDurationField == null) {
                            break;
                        }
                        long count = countField.asLong();
                        long avgDuration = avgDurationField.asLong();
                        long duration = avgDuration * count;
                        Long commulativeDuration = name2Instant.getOrDefault(name, new Long(0));
                        commulativeDuration = commulativeDuration + duration;
                        name2Instant.put(name, commulativeDuration);
                        break;
                    default:
                        System.err.println("Unknown phase of entry: " + phase);
                }
            }
        }

        // Post processing
        // Change all nanos to micor
        List<String> keys = new ArrayList<>(name2Instant.keySet());
        for (String name : keys) {
            long durationNano = name2Instant.get(name);
            name2Instant.put(name, durationNano / 1000L);
        }

        // Write output
        bw.write("Start = (" + globalStart + ")" + ": " + new Date(globalStart / 1000L) + "\n");
        bw.write("End = (" + globalEnd + ")" + ": " + new Date(globalEnd / 1000L) + "\n");
        long totalTime = globalEnd - globalStart;
        bw.write("Total time spent: " + totalTime + "us = " + (totalTime / 1000L) + "ms = " + (totalTime / 1000000L)
                + "s \n");
        for (Map.Entry<String, long[]> entry : name2Duration.entrySet()) {
            String name = entry.getKey();
            long[] stats = entry.getValue();
            bw.write(name + " took: " + stats[0] + "us which is " + ((double) stats[0] / (double) totalTime)
                    + " of the whole time... min = " + stats[1] + ", max = " + stats[2] + ", count = " + stats[3]
                    + "\n");
        }

        for (Map.Entry<String, Long> entry : name2Instant.entrySet()) {
            String name = entry.getKey();
            Long time = entry.getValue();
            bw.write(name + " took: " + time + "us which is " + ((double) time / (double) totalTime)
                    + " of the whole time\n");
        }
    }
}
