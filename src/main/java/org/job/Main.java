package org.job;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar <jar> <input-file.gz>");
            System.exit(1);
        }

        new GroupingProcessor().process(args[0]);
    }
}

