import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FileCounter {

    public static void main(String[] args) throws IOException {
        Properties properties = getProperties();
        if (properties.pathName() == null) {
            System.out.println("No pathName found");
            return;
        }
        final ConcurrentHashMap<String, Integer> fileNameGrouper = new ConcurrentHashMap<>();
        final AtomicInteger count = new AtomicInteger(0);
        var basePath = Path.of(properties.pathName());
        var fileNameGroupingIndex = properties.fileNameGroupingIndex();
        try (var stream = Files.walk(basePath)) {
            boolean groupFileCountsByName = properties.groupFileCountsByName();
            stream.parallel().filter(Files::isRegularFile)
                .forEach(path -> {
                    var file = path.toFile().getAbsolutePath();
                    var filePathWithoutBasePath = file.substring(basePath.toString().length() + 1);
                    if (groupFileCountsByName) {
                        String fileNameGroup = filePathWithoutBasePath.substring(0, fileNameGroupingIndex);
                        Integer fileNameGroupItemCount = fileNameGrouper.getOrDefault(fileNameGroup, 0);
                        fileNameGrouper.put(fileNameGroup, fileNameGroupItemCount + 1);
                    }
                    count.incrementAndGet();
                });
        }

        var path = Path.of("total-file-counts.csv");
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        Files.writeString(path, "pathName,count\n", StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(path, "Total Count," + count, StandardOpenOption.APPEND);

        if (properties.groupFileCountsByName()) {
            fileNameGrouper.forEach((k, v) -> {
                try {
                    Files.writeString(path, "\n" + k + "," + v, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        System.out.println(count);
    }

    private static Properties getProperties() throws IOException {
        String properties = Files.readString(Path.of("app.properties"));
        var propertyArray = properties.split("[;\n]");
        String pathName = null;
        boolean printFolderCounts = true;
        int fileNameGroupingIndex = 12;
        for (String property : propertyArray) {
            var keyValue = property.split("=");
            String propertyKey = keyValue[0];
            String propertyValue = keyValue[1].replaceAll("[\t\r]", "");
            if ("pathName".equals(propertyKey)) {
                pathName = propertyValue;
            } else if ("groupFileCountsByName".equals(propertyKey)) {
                printFolderCounts = Boolean.parseBoolean(propertyValue);
            } else if ("fileNameGroupingIndex".equals(propertyKey)) {
                fileNameGroupingIndex = Integer.parseInt(propertyValue);
            }
        }
        return new Properties(pathName, printFolderCounts, fileNameGroupingIndex);
    }

    public record Properties(String pathName, boolean groupFileCountsByName, int fileNameGroupingIndex) {

    }
}
