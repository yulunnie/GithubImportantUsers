import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager

/**
 * A UserRank use page rank algorithm to compute the attention of a github user
 */
object UserRank {

  def main(args: Array[String]) {
    // initiail the logger
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // checking arguments
    if (args.length != 2) {
      logger.error("Usage:\n<input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("UserRank")//.setMaster("local[4]")
    val sc = new SparkContext(conf)

    // parameter setting
    val dummy = -2        // a dummy to store leak values due to dangling nodes
    val root = -1         // a root node connect to all front node
    val max = 37700        // max node id to process, test use
    val initialRank = 1f  // initial UserRank
    val alpha = 0.15      // alpha value for random jump
    val iterations = 1    // total iteration times

    // create (dangling, dummy)
    val danglingToDummy = sc.textFile("input/danglings.csv")
      .filter( id => id.toInt < max )
      .map( id => (id.toInt, dummy) )

    // create (root, front)
    val rootToFront = sc.textFile("input/fronts.csv")
      .filter( id => id.toInt < max )
      .map( id => (root, id.toInt) )

    // load edges
    val edges = sc.textFile("input/musae_git_edges.csv")
      .map( line => line.split(",") )
      .map( fromTo => (fromTo(0).toInt, fromTo(1).toInt) )
      .filter{ case (from, to) => from < max && to < max }

    // create the complete graph
    val graph = edges.union(danglingToDummy).union(rootToFront)

    // create rank table
    var ranksList : List[(Int,Float)] = List()
    // rank of dummy and root is all 0
    ranksList = ranksList :+ (dummy, 0f)
    ranksList = ranksList :+ (root, 0f)
    for (i <- 0 to max) {
      // rank of all real node is 1
      ranksList = ranksList :+ (i, initialRank)
    }
    var ranks = sc.parallelize(ranksList, 20).partitionBy(new HashPartitioner(100))

    // iteration
    for (i <- 0 until iterations) {
      // generate a (id, rankValue) table for all nodes
      val contributions = graph.groupByKey().join(ranks).flatMap{
        case (id, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      // compute the leak/#node
      val distribution = contributions.lookup(0).sum / max
      ranks = contributions.reduceByKey(_ + _).mapValues(v => (alpha/max + (1f-alpha)*(v + distribution)).toFloat)
      ranks = ranks.union(sc.parallelize(List((root, 0f))))
    }

    logger.info(ranks.toDebugString)

    // output
    //ranks.repartition(1).saveAsTextFile(args(1))
    ranks.filter{ case(id, rank) => rank > 5 }.repartition(1).saveAsTextFile(args(1))
  }
}