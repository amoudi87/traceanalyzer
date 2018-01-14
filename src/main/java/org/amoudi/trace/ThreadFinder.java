package org.amoudi.trace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class ThreadFinder {

    private static final String THREAD_FIELD_NAME = "tid";
    private static final String PROCESS_FIELD_NAME = "pid";

    private static void help() {
        System.out.println("This tool is used to find threads that produces some traces");
        System.out.println("Note that it only works with String fields as of now");
        System.out.println("Expected arguments are:");
        System.out.println("-f <file name>");
        System.out.println("<field 1> <value 1> <field 2> <value 2> ... <field n> <value n> ");
    }

    public static List<Pair<String,Long>> find(String fileName, Map<String, List<String>> predicates) throws Exception {
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
        return find(input, predicates);
    }

    private static List<Pair<String, Long>> find(File file, Map<String, List<String>> predicates) throws Exception {
        ObjectMapper om = new ObjectMapper();
        Set<Pair<String, Long>> threadIds = new HashSet<>();
        Set<Map.Entry<String, List<String>>> entries = predicates.entrySet();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
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
                    continue;
                }
                JsonNode threadId = json.findValue(THREAD_FIELD_NAME);
                if (threadId == null) {
                    System.out.println(line + " doesn't contain a " + THREAD_FIELD_NAME + " object");
                    continue;
                }
                JsonNode processId = json.findValue(PROCESS_FIELD_NAME);
                for (Map.Entry<String, List<String>> entry : entries) {
                    JsonNode valueInJson = json.findValue(entry.getKey());
                    if (valueInJson != null && valueInJson.isTextual()) {
                        String valueAsString = valueInJson.asText();
                        for (String match : entry.getValue()) {
                            if (match.equals(valueAsString)) {
                                threadIds.add(Pair.of(processId.textValue(), threadId.longValue()));
                                break;
                            }
                        }
                    }
                }
            }
            System.out.println("Found the following matching threads: " + Arrays.toString(threadIds.toArray()));
        }
        return new ArrayList<>(threadIds);
    }
}
