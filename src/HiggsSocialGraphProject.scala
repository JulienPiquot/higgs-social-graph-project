import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.stat.Statistics

import scala.xml.{Elem, XML}

/**
  * Created by julien on 23/05/18.
  */
object HiggsSocialGraphProject {

  // mention : higgs-mention_network.edgelist

  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    val retweetG = readGraph(sc, "higgs-retweet_network.edgelist")
    val mentionG = readGraph(sc, "higgs-mention_network.edgelist")
    val replyG = readGraph(sc, "higgs-reply_network.edgelist")
    val activityG = readActivityGraph(sc, "higgs-activity_time.txt")

    /******************************************/
    /* comptage de l'effectif heure par heure */
    /******************************************/
    val hist: RDD[(Long, Int)] = activityG.edges.map(e => 3600 * (e.attr._1 / 3600)).groupBy(e => e).map(g => (g._1, g._2.size)).sortBy(e => e._1)
    // verification : le resultat du reduce = le nombre d'arêtes
    hist.map(e => e._2).reduce((e1, e2) => e1 + e2)
    val cHist = hist.collect()
    val effectifMin = cHist.map(e => e._2).min
    val effectifMax = cHist.map(e => e._2).max
    printHist(cHist, "dist_over_time.txt")

    /********************************/
    /* comptage des degres entrants */
    /********************************/
    val degreesCount: RDD[(Int, Int)] = effectifDegres(activityG.inDegrees)
    val maxDegre = activityG.inDegrees.map(e => e._2).max
    val moyDegres = activityG.inDegrees.map(e => e._2).reduce(_ + _) / activityG.inDegrees.count()
    printHist(degreesCount.collect(), "degrees_count.txt")

    /***************************************/
    /* calcul du coefficient de clustering */
    /***************************************/
    val triCountGraph = activityG.triangleCount()
    triCountGraph.vertices.map(x => x._2).stats()
    val maxTrisGraph = activityG.degrees.mapValues(d => d * (d - 1) / 2.0);
    val clusterCoefGraph: VertexRDD[Double] = triCountGraph.vertices.innerJoin(maxTrisGraph) {
      (vertexId, triCount, maxTris) => {
        if (maxTris == 0) 0
        else triCount / maxTris
      }
    }

    /****************************************************/
    /* calcul du coefficient de correlation de Spearman */
    /****************************************************/
    val clusterAndDegrees = clusterCoefGraph.innerJoin(activityG.inDegrees) {
      (vertexId, clusterCoef, degrees) => {
        (clusterCoef, degrees.toDouble)
      }
    }
    val correlation1 = Statistics.corr(clusterAndDegrees.map(v => v._2._1), clusterAndDegrees.map(v => v._2._2), "spearman")
    clusterAndDegrees.sortBy(v => v._2._2, false).take(10)

    /************/
    /* PageRank */
    /************/
    val pageRank: Graph[Double, Double] = activityG.pageRank(0.0001);
    // verifier que la somme totale des page ranks est égale au nombre de sommets
    val totalPR = pageRank.vertices.map(_._2).fold(0d)((a, b) => a + b) / pageRank.vertices.count
    val pagerankAndDegrees = pageRank.vertices.innerJoin(activityG.inDegrees) {
      (vertexId, clusterCoef, degrees) => {
        (clusterCoef, degrees.toDouble)
      }
    }
    val correlation2 = Statistics.corr(pagerankAndDegrees.map(v => v._2._1), pagerankAndDegrees.map(v => v._2._2), "spearman")
    pagerankAndDegrees.sortBy(v => v._2._2, false).take(10)

    /**********************/
    /* Gefx serialization */
    /**********************/
    val gefx: Elem = GefxFormater.format(replyG.vertices.collect(), replyG.edges.collect())
    val writer = new PrintWriter("higgs-social_network-pr.gexf")
    XML.write(writer, gefx, "utf-8", xmlDecl = true, doctype = null)
    writer.close()

    /*************************************************************************************************/
    /* Utiliser la fonction d'aggrégation afin de trouver le timestamp d'activation de chaque sommet */
    /*************************************************************************************************/
    val aggTimestamp: VertexRDD[Long] = activityG.aggregateMessages[Long](triplet => {
      triplet.sendToSrc(triplet.attr._1)
    }, (a: Long, b: Long) => if (a > b) b else a, TripletFields.EdgeOnly)
    val activityHist = aggTimestamp.map(e => 3600 * (e._2 / 3600)).groupBy(e => e).map(g => (g._1, g._2.size)).sortBy(e => e._1)
    printHist(activityHist.collect().map{var acc: Long = 0; v => {acc += v._2; (v._1, acc)}}, "activity_over_time.txt")

    sc.stop()
  }

  def effectifDegres(v: VertexRDD[Int]) = {
    v.map(x => (x._2, 1)).reduceByKey(_ + _).sortByKey()
  }

  def readActivityGraph(sparkContext: SparkContext, fileName: String): Graph[Null, (Long, String)] = {
    val rdd = sparkContext.textFile(fileName).map(line => line.split("\\s+") match {
      case Array(usr1, usr2, timestamp, interaction) => (usr1.toLong, usr2.toLong, timestamp.toLong, interaction)
    })
    val edges: RDD[Edge[(Long, String)]] = rdd.map(tuple => Edge(tuple._1, tuple._2, (tuple._3, tuple._4)))
    Graph.fromEdges(edges, null)
  }

  def readGraph(sparkContext: SparkContext, fileName: String): Graph[Null, Null] = {
    val rdd = sparkContext.textFile(fileName).map(line => line.split("\\s+") match {
      case Array(str1, str2, str3) => (str1.toLong, str2.toLong, str3.toLong)
    })
    val edges: RDD[Edge[Null]] = rdd.flatMap(tuple => replicate(Edge(tuple._1, tuple._2, tuple._3)))
    Graph.fromEdges(edges, null)
  }

  def printHist[T1, T2](hist: Array[(T1, T2)], fileName: String) = {
    val writer = new PrintWriter(fileName)
    hist.foreach(col => writer.append(col._1.toString).append(' ').append(col._2.toString).append('\n'))
    writer.close()
  }

  def replicate(edge: Edge[Long]): Array[Edge[Null]] = {
    Array.fill(edge.attr.toInt)(Edge(edge.srcId, edge.dstId, null))
  }

}
