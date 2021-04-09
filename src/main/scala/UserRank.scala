import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager

/**
 * A spark program to perform pageRank algorithm
 */
object UserRank {

  def main(args: Array[String]) {
    // initiail the logger
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // checking arguments
    if (args.length != 2) {
      logger.error("Usage:\nPageRank <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("PageRank")//.setMaster("local[4]")
    val sc = new SparkContext(conf)

    // parameter setting
    val dummy = 0
    val root = -1  // a root node connect to all front node of a chain
    val k = 10
    val initialPageRank = 1f / (k*k)
    val alpha = 0.15
    val iterations = 10

    // create synthetic graph
    var edges : List[(Int, Int)] = List()
    for (i <- 0 until k) {
      val offset = i * k
      // add edge (root, front)
      edges = edges :+ (root,offset+1)
      for (j <- 1 until k) {
        // append real edges
        edges = edges :+ (offset+j,offset+j+1)
      }
      // append (dangling, dummy)
      edges = edges :+ ((i+1) * k,dummy)
    }
    val graph = sc.parallelize(edges, 20).partitionBy(new HashPartitioner(100))

    // create page rank table
    var pageRanks : List[(Int,Float)] = List()
    // page rank of dummy and root is all 0
    pageRanks = pageRanks :+ (dummy, 0f)
    pageRanks = pageRanks :+ (root, 0f)
    for (i <- 1 to k*k) {
      // page rank of all real node is 1/k*k
      pageRanks = pageRanks :+ (i, initialPageRank)
    }
    var ranks = sc.parallelize(pageRanks, 20).partitionBy(new HashPartitioner(100))

    // iteration
    for (i <- 0 until iterations) {
      // generate a (pageId, pageRank) table for all nodes
      val contributions = graph.groupByKey().join(ranks).flatMap{
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      // compute the leak/#node
      val distribution = contributions.lookup(0).sum / (k*k)
      ranks = contributions.reduceByKey(_ + _).mapValues(v => (alpha/(k*k) + (1f-alpha)*(v + distribution)).toFloat)
      ranks = ranks.union(sc.parallelize(List((root, 0f))))
    }

    logger.info(ranks.toDebugString)

    // output
    //ranks.repartition(1).saveAsTextFile(args(1))
    ranks.filter{case (p, pr) => p < 20}.repartition(1).saveAsTextFile(args(1))
  }
}