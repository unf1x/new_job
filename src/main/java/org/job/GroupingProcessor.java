package org.job;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class GroupingProcessor {
    private static final Pattern BAD = Pattern.compile("\"\\d+\"\\d+");
    private final Map<String, String> pool = new HashMap<>();

    public void process(String inputFilePath) throws Exception {
        long startTime = System.nanoTime();
        Runtime runtime = Runtime.getRuntime();
        Path input = Paths.get(inputFilePath);

        List<String[]> rows = new ArrayList<>();

        Object2IntOpenHashMap<String> firstRow = new Object2IntOpenHashMap<>();
        firstRow.defaultReturnValue(-1);
        Object2ObjectOpenHashMap<String, IntArrayList> duplicates = new Object2ObjectOpenHashMap<>();

        try (BufferedReader reader = inputFilePath.endsWith(".gz")
                ? new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(input)), StandardCharsets.UTF_8))
                : Files.newBufferedReader(input)) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty() || BAD.matcher(line).find()) continue;
                String[] parts = line.split(";");
                for (int i = 0; i < parts.length; i++) {
                    String raw = parts[i].replace("\"", "").trim();
                    parts[i] = pool.computeIfAbsent(raw, k -> k);
                }
                int rowIndex = rows.size();
                rows.add(parts);

                for (int col = 0; col < parts.length; col++) {
                    String value = parts[col];
                    if (value.isEmpty()) continue;
                    String key = value + "#" + col;
                    int prev = firstRow.getInt(key);
                    if (prev == -1) {
                        firstRow.put(key, rowIndex);
                    } else {
                        IntArrayList list = duplicates.get(key);
                        if (list == null) {
                            list = new IntArrayList(4);
                            list.add(prev);
                            duplicates.put(key, list);
                        }
                        list.add(rowIndex);
                    }
                }
            }
        }

        UnionFind uf = new UnionFind(rows.size());
        for (IntArrayList indices : duplicates.values()) {
            if (indices.size() < 2) continue;
            int a = indices.getInt(0);
            for (int j = 1; j < indices.size(); j++) {
                uf.union(a, indices.getInt(j));
            }
        }

        Map<Integer, IntArrayList> groups = new HashMap<>(rows.size() / 10);
        for (int i = 0; i < rows.size(); i++) {
            int root = uf.find(i);
            groups.computeIfAbsent(root, k -> new IntArrayList()).add(i);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("output.txt"))) {
            long grpCount = groups.values().stream().filter(g -> g.size() > 1).count();
            writer.write("Групп больше чем из одной строки: " + grpCount);
            System.out.println("Групп больше чем из одной строки: " + grpCount);
            writer.newLine(); writer.newLine();

            int num = 1;
            for (IntArrayList g : groups.values()) {
                if (g.size() <= 1) continue;
                writer.write("Группа " + num++);
                writer.newLine();
                for (int idx : g) {
                    writer.write(String.join(";", rows.get(idx)));
                    writer.newLine();
                }
                writer.newLine();
            }
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        long peak = (runtime.totalMemory() - runtime.freeMemory()) / 1_048_576L;
        System.out.println("Пик мб: " + peak);
        System.out.println("Время мс: " + durationMs);
    }
}
