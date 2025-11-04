import javax.jws.soap.SOAPBinding.Use;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class PageRank {

    public static JavaPairRDD<Integer, Double> idealPageRank(JavaPairRDD<Integer, java.util.List<Integer>> linksRDD, JavaPairRDD<Integer, Double> ranksRDD, int numIterations) {

        //Implement ideal PageRank logic
        //Parameters:
        //linksRDD -> (fromPageId, [toPageIds])
        //ranksRDD -> (pageId, rank)
        //numIterations -> number of iterations
        //my thoguhts but I could be wrong:
        //Use linksRDD.join(ranksRDD), flatMapToPair to distribute ranks, and reduceByKey to sum contributions,Return the final ranksRDD after all iterations
        return ranksRDD; // placeholder
    }

    public static JavaPairRDD<Integer, Double> taxationPageRank( JavaPairRDD<Integer, java.util.List<Integer>> linksRDD, JavaPairRDD<Integer, Double> ranksRDD,int numIterations, double beta) {

        // Thoughts: Add teleportation probability for pages with no out-links, return the final ranksRDD after all iterations
        // Parameters:
        // beta -> damping factor, usually 0.85
        return ranksRDD; // placeholder
    }
}
