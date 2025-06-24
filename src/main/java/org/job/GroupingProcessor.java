package org.job;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class GroupingProcessor {
    public void process(String inputFilePath) throws Exception {
        long startTime = System.nanoTime();
        Path input = Paths.get(inputFilePath);

        List<List<String>> rows = new ArrayList<>();
        Map<String, List<Integer>> valueColumnToRows = new HashMap<>();


        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(Files.newInputStream(input)), "UTF-8"))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                if (line.matches(".*\"\\d+\"\\d+.*")) continue;

                String[] parts = line.split(";");
                List<String> cleaned = new ArrayList<>();
                for (String part : parts) {
                    cleaned.add(part.replace("\"", "").trim());
                }
                int rowIndex = rows.size();
                rows.add(cleaned);

                for (int i = 0; i < cleaned.size(); i++) {
                    String value = cleaned.get(i);
                    if (!value.isEmpty()) {
                        String key = value + "#" + i;
                        valueColumnToRows.computeIfAbsent(key, k -> new ArrayList<>()).add(rowIndex);
                    }
                }
            }
        }


        UnionFind uf = new UnionFind(rows.size());
        for (List<Integer> indices : valueColumnToRows.values()) {
            for (int i = 1; i < indices.size(); i++) {
                uf.union(indices.get(0), indices.get(i));
            }
        }


        Map<Integer, Set<Integer>> groups = new HashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            int root = uf.find(i);
            groups.computeIfAbsent(root, k -> new HashSet<>()).add(i);
        }


        List<List<String>> resultGroups = new ArrayList<>();
        for (Set<Integer> group : groups.values()) {
            if (group.size() > 1) {
                List<String> groupLines = new ArrayList<>();
                for (int idx : group) {
                    groupLines.add(String.join(";", rows.get(idx)));
                }
                Collections.sort(groupLines);
                resultGroups.add(groupLines);
            }
        }


        resultGroups.sort((a, b) -> {
            int countB = totalNonEmptyElements(b);
            int countA = totalNonEmptyElements(a);
            return Integer.compare(countB, countA);
        });


        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("output.txt"))) {
            writer.write("Групп больше чем из одной строки: " + resultGroups.size());
            writer.newLine();
            writer.newLine();

            int groupNum = 1;
            for (List<String> group : resultGroups) {
                writer.write("Группа " + groupNum++);
                writer.newLine();
                for (String line : group) {
                    writer.write(line);
                    writer.newLine();
                }
                writer.newLine();
            }
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Групп больше чем из одной строки: " + resultGroups.size());
        System.out.println("Время мс: " + durationMs);
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // Запускаем сборщик мусора для более точного измерения

        long usedMemoryBytes = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory (MB): " + usedMemoryBytes / (1024 * 1024));
    }

    private int totalNonEmptyElements(List<String> group) {
        int count = 0;
        for (String line : group) {
            for (String part : line.split(";")) {
                if (!part.trim().isEmpty()) {
                    count++;
                }
            }
        }
        return count;
    }
}
