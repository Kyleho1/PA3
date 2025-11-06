import java.util.ArrayList;
import java.util.List;

import javax.jws.soap.SOAPBinding.Use;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class PageRank {

    public static JavaPairRDD<Integer, Double> idealPageRank(
            JavaPairRDD<Integer, List<Integer>> linksRDD, 
            JavaPairRDD<Integer, Double> ranksRDD, 
            int numIterations) {
        
        for (int i = 0; i < numIterations; i++) {
            JavaPairRDD<Integer, Double> contributions = linksRDD.join(ranksRDD)
                .flatMapToPair(tuple -> {
                    Integer pageId = tuple._1();
                    List<Integer> outLinks = tuple._2()._1();
                    Double rank = tuple._2()._2();
                    
                    List<Tuple2<Integer, Double>> results = new ArrayList<>();
                    
                    int numOutLinks = outLinks.size();
                    
                    if (numOutLinks > 0) {
                        double contribution = rank / numOutLinks;
                        for (Integer link : outLinks) {
                            results.add(new Tuple2<>(link, contribution));
                        }
                    }
                    
                    return results.iterator();
                });
            
            ranksRDD = linksRDD.leftOuterJoin(contributions.reduceByKey((a, b) -> a + b))
                .mapValues(tuple -> {
                    return tuple._2().isPresent() ? tuple._2().get() : 0.0;
                });
            
            if ((i + 1) % 5 == 0) {
                System.out.println("Ideal PageRank - Iteration " + (i + 1) + " completed");
            }
        }
        
        return ranksRDD;
    }

     public static JavaPairRDD<Integer, Double> taxationPageRank(
            JavaPairRDD<Integer, List<Integer>> linksRDD, 
            JavaPairRDD<Integer, Double> ranksRDD,
            int numIterations, 
            double beta,
            long numPages) {
        
        double taxationFactor = (1.0 - beta) / numPages;
        
        for (int i = 0; i < numIterations; i++) {
            // Calculate contributions from following links
            JavaPairRDD<Integer, Double> contributions = linksRDD.join(ranksRDD)
                .flatMapToPair(tuple -> {
                    Integer pageId = tuple._1();
                    List<Integer> outLinks = tuple._2()._1();
                    Double rank = tuple._2()._2();
                    
                    List<Tuple2<Integer, Double>> results = new ArrayList<>();
                    
                    int numOutLinks = outLinks.size();
                    
                    if (numOutLinks > 0) {
                        double contribution = rank / numOutLinks;
                        for (Integer link : outLinks) {
                            results.add(new Tuple2<>(link, contribution));
                        }
                    }
                    
                    return results.iterator();
                });
            
            // Apply taxation formula
            ranksRDD = linksRDD.leftOuterJoin(contributions.reduceByKey((a, b) -> a + b))
                .mapValues(tuple -> {
                    double sum = tuple._2().isPresent() ? tuple._2().get() : 0.0;
                    return beta * sum + taxationFactor;
                });
            
            if ((i + 1) % 5 == 0) { // Print every 5 iterations
                System.out.println("Taxation PageRank - Iteration " + (i + 1) + " completed");
            }
        }
        
        return ranksRDD;
    }
}
