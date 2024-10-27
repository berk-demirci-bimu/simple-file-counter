import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FileCounter {

    public static void main(String[] args) throws IOException, ParseException {
        Properties properties = getProperties();
        if (properties.pathName() == null) {
            System.out.println("No pathName found");
            return;
        }
        var logPath = Path.of("read-process.log");
        if (!Files.exists(logPath)) {
            Files.createFile(logPath);
        }
        Files.writeString(logPath, getProcessingLogString(0), StandardOpenOption.TRUNCATE_EXISTING);
        final ConcurrentHashMap<String, Integer> fileNameGrouper = new ConcurrentHashMap<>();
        final AtomicInteger count = new AtomicInteger(0);
        var basePath = Path.of(properties.pathName());
        var fileNameGroupingIndex = properties.fileNameGroupingIndex();
        long createdAtMax = new SimpleDateFormat("dd-MM-yyyy'T'HH:mm:ss").parse(properties.createdAtMax()).getTime();
        boolean groupFileCountsByName = properties.groupFileCountsByName();
        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.creationTime().toMillis() < createdAtMax) {
                    var filePath = file.toFile().getAbsolutePath();
                    var filePathWithoutBasePath = filePath.substring(basePath.toString().length() + 1);
                    if (groupFileCountsByName) {
                        String fileNameGroup = filePathWithoutBasePath.substring(0, fileNameGroupingIndex);
                        Integer fileNameGroupItemCount = fileNameGrouper.getOrDefault(fileNameGroup, 0);
                        fileNameGrouper.put(fileNameGroup, fileNameGroupItemCount + 1);
                    }
                    int totalProcessedCount = count.incrementAndGet();
                    if (totalProcessedCount % 5000 == 0) {
                        try {
                            Files.writeString(logPath, "\n" + getProcessingLogString(totalProcessedCount), StandardOpenOption.APPEND);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                return super.visitFile(file, attrs);
            }
        });

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

    private static String getProcessingLogString(int totalProcessedCount) {
        return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " - total processed count: " + totalProcessedCount;
    }

    private static Properties getProperties() throws IOException {
        String properties = Files.readString(Path.of("app.properties"));
        var propertyArray = properties.split("[;\n]");
        String pathName = null;
        boolean printFolderCounts = true;
        int fileNameGroupingIndex = 12;
        String lastReadDate = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        for (String property : propertyArray) {
            var keyValue = property.split("=");
            String propertyKey = keyValue[0];
            String propertyValue = keyValue[1].replaceAll("[\t\r]", "");
            switch (propertyKey) {
                case "pathName" -> pathName = propertyValue;
                case "groupFileCountsByName" -> printFolderCounts = Boolean.parseBoolean(propertyValue);
                case "fileNameGroupingIndex" -> fileNameGroupingIndex = Integer.parseInt(propertyValue);
                case "lastReadDate" -> lastReadDate = propertyValue;
            }
        }
        return new Properties(pathName, printFolderCounts, fileNameGroupingIndex, lastReadDate);
    }

    public record Properties(String pathName, boolean groupFileCountsByName, int fileNameGroupingIndex, String createdAtMax) {

    }
}
