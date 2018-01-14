package org.amoudi.trace;

import org.apache.commons.lang3.tuple.Triple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class TreeHeightComputer {
    private static final String FLUSH = "flush -> ";
    private static final String MERGE = "merge -> ";

    private static double compute(String input) throws Exception {
        File file = new File(input);
        if (!file.exists()) {
            throw new Exception("File s" + file.getAbsolutePath() + " doesn't exist");
        }
        // Key is range start, value is Start, End, Height
        Map<Integer, Triple<Integer, Integer, Double>> weightedAvgMap = new HashMap<>();
        // Key is range start, value is Start, End, Height
        Map<Integer, Triple<Integer, Integer, Integer>> maxMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            for (String line; (line = br.readLine()) != null;) {
                if (line.length() == 0) {
                    break;
                }
                if (line.charAt(0) == 'm') {
                    int[] range = getMerge(line);
                    int maxHeightIn = 0;
                    double totalHeightIn = 0;
                    double countComponentsIn = 0;
                    for (int i = range[0]; i <= range[1]; i++) {
                        // The max
                        Triple<Integer, Integer, Integer> maxHeight = maxMap.remove(i);
                        if (maxHeight != null) {
                            maxHeightIn = Integer.max(maxHeightIn, maxHeight.getRight());
                            Triple<Integer, Integer, Double> avgHeight = weightedAvgMap.remove(i);
                            double thisCount = avgHeight.getMiddle() - avgHeight.getLeft() + 1;
                            countComponentsIn += thisCount;
                            double thisHeight = avgHeight.getRight() * thisCount;
                            totalHeightIn += thisHeight;
                        }
                    }
                    maxHeightIn++;
                    maxMap.put(range[0], Triple.of(range[0], range[1], maxHeightIn));
                    totalHeightIn /= countComponentsIn;
                    totalHeightIn += 1.0;
                    weightedAvgMap.put(range[0], Triple.of(range[0], range[1], totalHeightIn));
                } else if (line.charAt(0) == 'f') {
                    int number = getFlush(line);
                    weightedAvgMap.put(number, Triple.of(number, number, 1.0));
                    maxMap.put(number, Triple.of(number, number, 1));
                } else {
                    System.err.println("Line " + line + " doesn't representa flush nor a merge");
                    help();
                    System.exit(1);
                }
            }
        }
        System.out.println("The height computation using the max of input +1 = the height of output");
        int maxMax = 0;
        for (Map.Entry<Integer, Triple<Integer, Integer, Integer>> entry : maxMap.entrySet()) {
            maxMax = Integer.max(maxMax, entry.getValue().getRight());
            System.out.println("[" + entry.getValue().getLeft() + '-' + entry.getValue().getMiddle() + "] -> "
                    + entry.getValue().getRight());
        }
        System.out.println("The max height = " + maxMax);
        System.out.println("==========================================================================");
        System.out.println("==========================================================================");
        System.out.println("==========================================================================");
        System.out.println("The height computation using the weighted average of input +1 = the height of output");
        double avgMax = 0;
        for (Map.Entry<Integer, Triple<Integer, Integer, Double>> entry : weightedAvgMap.entrySet()) {
            avgMax = Double.max(avgMax, entry.getValue().getRight());
            System.out.println("[" + entry.getValue().getLeft() + '-' + entry.getValue().getMiddle() + "] -> "
                    + entry.getValue().getRight());
        }
        return avgMax;
    }

    private static int[] getMerge(String line) {
        String rangeString = line.substring(MERGE.length()).trim();
        int delimiter = rangeString.indexOf('-');
        int[] range = new int[2];
        range[0] = Integer.parseInt(rangeString.substring(0, delimiter));
        range[1] = Integer.parseInt(rangeString.substring(delimiter + 1));
        return range;
    }

    private static int getFlush(String line) {
        return Integer.parseInt(line.substring(FLUSH.length()).trim());
    }

    private static void help() {
        System.out.println("This tool is used to compute the height of the tree of flushes and merges."
                + " The file passed is expected to only have flushes and merges as follows");
        System.out.println("flush -> 0");
        System.out.println("flush -> 1");
        System.out.println("flush -> 2");
        System.out.println("flush -> 3");
        System.out.println("flush -> 4");
        System.out.println("merge -> 0-4");
        System.out.println("flush -> 5");
        System.out.println("flush -> 6");
        System.out.println("    ...");
        System.out.println("Expected argument: <file name>");
    }
}
