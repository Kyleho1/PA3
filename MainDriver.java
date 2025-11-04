import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.List;

public class MainDriver {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WikipediaPageRank");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load input files
        JavaPairRDD<Long, String> titlesRDD = DataLoader.loadTitles(sc, "titles-sorted.txt");
        JavaPairRDD<Integer, List<Integer>> linksRDD = DataLoader.loadLinks(sc, "links-simple-sorted.txt");

        long numPages = titlesRDD.count();

        // Intializae the pages with a equal rank 
        JavaPairRDD<Integer, Double> ranksRDD = titlesRDD.mapToPair(x -> new Tuple2<>(x._1().intValue(), 1.0 / numPages));
        
        ranksRDD = PageRank.idealPageRank(linksRDD, ranksRDD, 25);
        // ranksRDD = PageRank.taxationPageRank(linksRDD, ranksRDD, 25, 0.85);

        // Join the ranks with titles and sort by rank descending
        JavaPairRDD<Double, String> finalRanks = ranksRDD.join(titlesRDD.mapToPair(x -> new Tuple2<>(x._1().intValue(), x._2()))).mapToPair(x -> new Tuple2<>(x._2()._1, x._2()._2)).sortByKey(false);

        //top 20 results
        List<Tuple2<Double, String>> topPages = finalRanks.take(20);
        for (Tuple2<Double, String> entry : topPages) {
            System.out.println(entry._2 + ": " + entry._1);
        }

        sc.stop();
    }
}

