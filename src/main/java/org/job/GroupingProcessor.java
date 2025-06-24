package org.job;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class GroupingProcessor {
    public void process(String inputFilePath) throws Exception {
        long startTime = System.nanoTime();
        Path input = Paths.get(inputFilePath);

        List<String[]> rows = new ArrayList<>();
        Map<String, List<Integer>> valueColumnToRows = new HashMap<>();

        BufferedReader reader;
        if (inputFilePath.endsWith(".gz")) {
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(input)), "UTF-8"));
        } else {
            reader = Files.newBufferedReader(Paths.get(inputFilePath));
        }

        try (reader ) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                if (line.matches(".*\"\\d+\"\\d+.*")) continue;

                String[] parts = line.split(";");
                for (int i = 0; i < parts.length; i++) {
                    parts[i] = parts[i].replace("\"", "").trim();
                }
                int rowIndex = rows.size();
                rows.add(parts);

                for (int i = 0; i < parts.length; i++) {
                    String value = parts[i];
                    if (!value.isEmpty()) {
                        String key = value + "#" + i;
                        valueColumnToRows.computeIfAbsent(key, k -> new ArrayList<>()).add(rowIndex);
                    }
                }
            }
        }

        UnionFind uf = new UnionFind(rows.size());
        for (List<Integer> indices : valueColumnToRows.values()) {
            int first = indices.get(0);
            for (int i = 1; i < indices.size(); i++) {
                uf.union(first, indices.get(i));
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
                Set<String> groupLines = new TreeSet<>();
                for (int idx : group) {
                    groupLines.add(String.join(";", rows.get(idx)));
                }
                resultGroups.add(new ArrayList<>(groupLines));
            }
        }

        resultGroups.sort((a, b) -> Integer.compare(
                b.stream().mapToInt(s -> (int) Arrays.stream(s.split(";")).filter(p -> !p.isEmpty()).count()).sum(),
                a.stream().mapToInt(s -> (int) Arrays.stream(s.split(";")).filter(p -> !p.isEmpty()).count()).sum()
        ));

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
        runtime.gc();
        long usedMemoryBytes = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory (MB): " + usedMemoryBytes / (1024 * 1024));
    }
}
