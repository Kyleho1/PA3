import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DataLoader {

    // Load (pageId, title) pairs from the titles file
    public static JavaPairRDD<Long, String> loadTitles(JavaSparkContext sc, String filePath) {
        JavaRDD<String> lines = sc.textFile(filePath);

        //zipWithIndex gives (line, index), so flip to (index, line)
        return lines.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2, x._1.trim()));
    }

    //Load (fromId, [toIds]) pairs from the links file
    public static JavaPairRDD<Integer, List<Integer>> loadLinks(JavaSparkContext sc, String filePath) {
        JavaRDD<String> lines = sc.textFile(filePath);

        return lines.mapToPair(line -> {
            String[] parts = line.split(":");
            int fromId = Integer.parseInt(parts[0].trim());

            //Split the destination IDs and parse them into a list
            List<Integer> toIds = Arrays.stream(parts[1].trim().split("\\s+")).filter(s -> !s.isEmpty()).map(Integer::parseInt).collect(Collectors.toList());

            return new Tuple2<>(fromId, toIds);
        });
    }
}
