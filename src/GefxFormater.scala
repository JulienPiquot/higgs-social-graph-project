import org.apache.spark.graphx._

import scala.xml.{Elem, XML}

object GefxFormater {

  def format[T1, T2](vertices: Array[(VertexId, T1)], edges: Array[Edge[T2]]): Elem = {
    val gefx: Elem =
      <gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
        <graph mode="static" defaultedgetype="directed">
          <nodes>
            {vertices.map(vertex => <node id={vertex._1.toString} label={vertex._1.toString}/>)}
          </nodes>
          <edges>
            {edges.zipWithIndex.map(edgeWithIndex => <edge id={edgeWithIndex._2.toString} source={edgeWithIndex._1.srcId.toString} target={edgeWithIndex._1.dstId.toString}/>)}
          </edges>
        </graph>
      </gexf>

    gefx
  }

  def main(args: Array[String]): Unit = {
    val vertices: Array[(VertexId, Double)] = Array((0l, 0.0d), (1l, 0.0d), (2l, 0.0d))
    val edges: Array[Edge[Double]] = Array(Edge(0l, 1l, 0.0d), Edge(1l, 2l, 0.0d));
    println(GefxFormater.format(vertices, edges))

  }

}
