/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph._
import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.rdd._
import org.scalatest.FunSuite

class GraphOpsSuite extends FunSuite with LocalSparkContext {

  test("joinVertices") {
    withSpark { sc =>
      val vertices =
        sc.parallelize(Seq[(VertexId, String)]((1, "one"), (2, "two"), (3, "three")), 2)
      val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
      val g: Graph[String, String] = Graph(vertices, edges)

      val tbl = sc.parallelize(Seq[(VertexId, Int)]((1, 10), (2, 20)))
      val g1 = g.joinVertices(tbl) { (vid: VertexId, attr: String, u: Int) => attr + u }

      val v = g1.vertices.collect().toSet
      assert(v === Set((1, "one10"), (2, "two20"), (3, "three")))
    }
  }

  test("collectNeighborIds") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val nbrs = graph.collectNeighborIds(EdgeDirection.Either).cache()
      assert(nbrs.count === 100)
      assert(graph.numVertices === nbrs.count)
      nbrs.collect.foreach { case (vid, nbrs) => assert(nbrs.size === 2) }
      nbrs.collect.foreach { case (vid, nbrs) =>
        val s = nbrs.toSet
        assert(s.contains((vid + 1) % 100))
        assert(s.contains(if (vid > 0) vid - 1 else 99 ))
      }
    }
  }

  test("collectEdgesCycleDirectionOut") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach { case (vid, edges) => 
        val s = edges.toSet
        val edgeDstIds = s.map(e => e.dstId)
        assert(edgeDstIds.contains((vid + 1) % 100))
      }
    }
  }
  
  test("collectEdgesCycleDirectionIn") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.In).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach { case (vid, edges) => 
        val s = edges.toSet
        val edgeSrcIds = s.map(e => e.srcId)
        assert(edgeSrcIds.contains(if (vid > 0) vid - 1 else 99 ))
      }
    }
  }
  
  test("collectEdgesCycleDirectionEither") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 2) }
      edges.collect.foreach { case (vid, edges) => 
        val s = edges.toSet
        val edgeIds = s.map(e => if (vid != e.srcId) e.srcId else e.dstId)
        assert(edgeIds.contains((vid + 1) % 100))
        assert(edgeIds.contains(if (vid > 0) vid - 1 else 99 ))
      }
    }
  }
  
  test("collectEdgesChainDirectionOut") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 49)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach { case (vid, edges) => 
        val s = edges.toSet
        val edgeDstIds = s.map(e => e.dstId)
        assert(edgeDstIds.contains(vid + 1))
      }
    }
  }
  
  test("collectEdgesChainDirectionIn") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.In).cache()
      // We expect only 49 because collectEdges does not return vertices that do
      // not have any edges in the specified direction.
      assert(edges.count == 49)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach { case (vid, edges) => 
        val s = edges.toSet
        val edgeDstIds = s.map(e => e.srcId)
        assert(edgeDstIds.contains((vid - 1) % 100))
      }
    }
  }
  
  test("collectEdgesChainDirectionBoth") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      // We expect only 49 because collectEdges does not return vertices that do
      // not have any edges in the specified direction.
      assert(edges.count === 50)
      edges.collect.foreach { case (vid, edges) => if (vid > 0 && vid < 49) assert(edges.size == 2) 
        else assert(edges.size == 1)}
      edges.collect.foreach { case (vid, edges) => 
        val s = edges.toSet
        val edgeIds = s.map(e => if (vid != e.srcId) e.srcId else e.dstId)
        if (vid == 0) { assert(edgeIds.contains(1)) }
        else if (vid == 49) { assert(edgeIds.contains(48)) }
        else {
          assert(edgeIds.contains(vid + 1))
          assert(edgeIds.contains(vid - 1))
        }
      }
    }
  }

  test("mapVerticesUsingLocalEdges") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      // we set the value of singleton vertices to -1, vertices with one edge to the 
      // srcId of the e, and others to Long.MaxValue. In the end we expect:
      // 1 vertex with value -1
      // all others with value (vId- 1)
      // no vertex with value Long.MaxValue
      val newGraph = graph.mapVerticesUsingLocalEdges[VertexId](EdgeDirection.In, 
        (vid, vdata, edges) => {
          if (edges.isEmpty) { -1 }
          else if (edges.length == 1) { edges(0).srcId }
          else { Long.MaxValue }
        }).cache()
      assert(50 == newGraph.numVertices)
      assert(49 == newGraph.numEdges)
      newGraph.vertices.collect.foreach { case (vid, value) => 
        if (vid == 0) assert(-1 == value) else assert(vid - 1 == value) }
    }
  }

  test ("filter") {
    withSpark { sc =>
      val n = 5
      val vertices = sc.parallelize((0 to n).map(x => (x:VertexId, x)))
      val edges = sc.parallelize((1 to n).map(x => Edge(0, x, x)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filter(
        graph => {
          val degrees: VertexRDD[Int] = graph.outDegrees
          graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
        },
        vpred = (vid: VertexId, deg:Int) => deg > 0
      ).cache()
      val numEdges =filteredGraph.numEdges
      val v = filteredGraph.vertices.collect().toSet
      assert(v === Set((0,0)))

      // the map is necessary because of object-reuse in the edge iterator
      val e = filteredGraph.edges.map(e => Edge(e.srcId, e.dstId, e.attr)).collect().toSet
      assert(e.isEmpty)
    }
  }

  test("filterEdges") {
    withSpark { sc =>
      // 5 vertices, every vertex points at 0, 1 points at every vertex, 2 also points at 3.
      // Edge (u, v) has value u + v
      val edges = sc.parallelize((1 to 1).flatMap(x => {
        Vector(Edge(0, 1, 1), Edge(1, 0, 1), Edge(1, 2, 3), Edge(1, 3, 4), Edge(1,4, 5),
          Edge(2, 3, 5), Edge(2, 0, 2), Edge(3, 0, 3), Edge(4, 0, 4)) }
      ))
      // Vertex i have value i
      val vertices = sc.parallelize((0 to 4).map(vid => (vid:VertexId, vid)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filterEdges(edgeTriplet => (edgeTriplet.attr == 3))
      assert(5 == filteredGraph.numVertices)
      val collectedVertices = filteredGraph.vertices.collect
      assert(collectedVertices.contains((0, 0)))
      assert(collectedVertices.contains((1, 1)))
      assert(collectedVertices.contains((2, 2)))
      assert(collectedVertices.contains((3, 3)))
      assert(collectedVertices.contains((4, 4)))
      // We only expect the following edges: (1, 2, 3) and (3, 0, 3)
      assert(2 == filteredGraph.numEdges)
      val filteredEdges = filteredGraph.edges.collect()
      assert(filteredEdges.contains(Edge(1, 2, 3)))
      assert(filteredEdges.contains(Edge(3, 0, 3)))
    }
  }

  test("filterVertices") {
    withSpark { sc =>
      // 5 vertices, every vertex points at 0, 1 points at every vertex, 2 also points at 3.
      // Edge (u, v) has value u + v
      val edges = sc.parallelize((1 to 1).flatMap(x => {
        Vector(Edge(0, 1, 1), Edge(1, 0, 1), Edge(1, 2, 3), Edge(1, 3, 4), Edge(1,4, 5),
          Edge(2, 3, 5), Edge(2, 0, 2), Edge(3, 0, 3), Edge(4, 0, 4)) }
      ))
      // Vertex i have value i
      val vertices = sc.parallelize((0 to 4).map(vid => (vid:VertexId, vid)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filterVertices((vid, vval) => (vval <= 2))
      assert(3 == filteredGraph.numVertices)
      val collectedVertices = filteredGraph.vertices.collect
      assert(collectedVertices.contains((0, 0)))
      assert(collectedVertices.contains((1, 1)))
      assert(collectedVertices.contains((2, 2)))
      // We only expect the following edges: (1, 2, 3) and (3, 0, 3)
      assert(4 == filteredGraph.numEdges)
      val filteredEdges = filteredGraph.edges.collect()
      assert(filteredEdges.contains(Edge(0, 1, 1)))
      assert(filteredEdges.contains(Edge(1, 0, 1)))
      assert(filteredEdges.contains(Edge(1, 2, 3)))
      assert(filteredEdges.contains(Edge(2, 0, 2)))
    }
  }
    
  test("filterVerticesUsingLocalEdges") {
    withSpark { sc =>
      // 5 vertices, every vertex points at 0, 1 points at every vertex, 2 also points at 3.
      // All edges have value -1
      val edges = sc.parallelize((1 to 1).flatMap(x => {
        Vector(Edge(0, 1, -1), Edge(1, 0, -1), Edge(1, 2, -1), Edge(1, 3, -1), Edge(1,4, -1),
          Edge(2, 3, -1), Edge(2, 0, -1), Edge(3, 0, -1), Edge(4, 0, -1)) }
      ))
      // Vertex i have value i
      val vertices = sc.parallelize((0 to 4).map(vid => (vid:VertexId, vid)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filterVerticesUsingLocalEdgesWithMask(EdgeDirection.Out,
        (vid, vdata, edges) => edges.length >= 2)
      assert(2 == filteredGraph.numVertices)
      val collectedVertices = filteredGraph.vertices.collect
      assert(collectedVertices.contains((1, 1)))
      assert(collectedVertices.contains((2, 2)))
      assert(1 == filteredGraph.numEdges)
      assert(Edge(1, 2, -1) == filteredGraph.edges.collect()(0))
    }
  }

  private def getCycleGraph(sc: SparkContext, numVertices: Int): Graph[Double, Int] = {
    val cycle = (0 until numVertices).map(x => (x, (x+1)%numVertices))
    getGraphFromSeq(sc, cycle)
  }
  
  private def getChainGraph(sc: SparkContext, numVertices: Int): Graph[Double, Int] = {
    val chain = (0 until numVertices-1).map(x => (x, (x+1)))
    getGraphFromSeq(sc, chain)
  }
  
  private def getGraphFromSeq(sc: SparkContext, seq: IndexedSeq[(Int, Int)]): Graph[Double, Int] = {
    val rawEdges = sc.parallelize(seq, 3).map { case (s,d) => (s.toLong, d.toLong) }
    Graph.fromEdgeTuples(rawEdges, 1.0).cache()
  }
}
