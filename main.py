#driver code

from pyspark import SparkContext
from data_loader import load_titles, load_links
from pagerank import ideal_pagerank, taxation_pagerank  

if __name__ == "__main__":
    sc = SparkContext(appName="WikipediaPageRank")

    titles_rdd = load_titles(sc, "titles-sorted.txt")
    links_rdd = load_links(sc, "links-simple-sorted.txt")

    num_pages = titles_rdd.count()
    ranks_rdd = titles_rdd.mapValues(lambda _: 1.0 / num_pages)

    # Choose algorithm
    ranks_rdd = ideal_pagerank(links_rdd, ranks_rdd, 25)
    # ranks_rdd = taxation_pagerank(links_rdd, ranks_rdd, 25, beta=0.85)

    final_ranks = titles_rdd.join(ranks_rdd).map(lambda x: (x[1][0], x[1][1])).sortBy(lambda x: -x[1])  # sort by rank descending order i think should be done

    for rank, title in final_ranks.take(20):
        print(f"{title}: {rank}")

    sc.stop()

