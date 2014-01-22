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
      nbrs.collect.foreach {
        case (vid, nbrs) =>
          val s = nbrs.toSet
          assert(s.contains((vid + 1) % 100))
          assert(s.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("collectEdgesCycleDirectionOut") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
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
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeSrcIds = s.map(e => e.srcId)
          assert(edgeSrcIds.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("collectEdgesCycleDirectionEither") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 2) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeIds = s.map(e => if (vid != e.srcId) e.srcId else e.dstId)
          assert(edgeIds.contains((vid + 1) % 100))
          assert(edgeIds.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("collectEdgesChainDirectionOut") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 49)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
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
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeDstIds = s.map(e => e.srcId)
          assert(edgeDstIds.contains((vid - 1) % 100))
      }
    }
  }

  test("collectEdgesChainDirectionEither") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      // We expect only 49 because collectEdges does not return vertices that do
      // not have any edges in the specified direction.
      assert(edges.count === 50)
      edges.collect.foreach {
        case (vid, edges) => if (vid > 0 && vid < 49) assert(edges.size == 2)
        else assert(edges.size == 1)
      }
      edges.collect.foreach {
        case (vid, edges) =>
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
      newGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(-1 == value) else assert(vid - 1 == value)
      }
    }
  }

  test("filter") {
    withSpark { sc =>
      val n = 5
      val vertices = sc.parallelize((0 to n).map(x => (x: VertexId, x)))
      val edges = sc.parallelize((1 to n).map(x => Edge(0, x, x)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filter(
        graph => {
          val degrees: VertexRDD[Int] = graph.outDegrees
          graph.outerJoinVertices(degrees) { (vid, data, deg) => deg.getOrElse(0) }
        },
        vpred = (vid: VertexId, deg: Int) => deg > 0).cache()
      val numEdges = filteredGraph.numEdges
      val v = filteredGraph.vertices.collect().toSet
      assert(v === Set((0, 0)))

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
        Vector(Edge(0, 1, 1), Edge(1, 0, 1), Edge(1, 2, 3), Edge(1, 3, 4), Edge(1, 4, 5),
          Edge(2, 3, 5), Edge(2, 0, 2), Edge(3, 0, 3), Edge(4, 0, 4))
      }))
      // Vertex i have value i
      val vertices = sc.parallelize((0 to 4).map(vid => (vid: VertexId, vid)))
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
        Vector(Edge(0, 1, 1), Edge(1, 0, 1), Edge(1, 2, 3), Edge(1, 3, 4), Edge(1, 4, 5),
          Edge(2, 3, 5), Edge(2, 0, 2), Edge(3, 0, 3), Edge(4, 0, 4))
      }))
      // Vertex i have value i
      val vertices = sc.parallelize((0 to 4).map(vid => (vid: VertexId, vid)))
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
        Vector(Edge(0, 1, -1), Edge(1, 0, -1), Edge(1, 2, -1), Edge(1, 3, -1), Edge(1, 4, -1),
          Edge(2, 3, -1), Edge(2, 0, -1), Edge(3, 0, -1), Edge(4, 0, -1))
      }))
      // Vertex i have value i
      val vertices = sc.parallelize((0 to 4).map(vid => (vid: VertexId, vid)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filterVerticesUsingLocalEdges(EdgeDirection.Out,
        (vid, vdata, edges) => edges.length >= 2)
      assert(2 == filteredGraph.numVertices)
      val collectedVertices = filteredGraph.vertices.collect
      assert(collectedVertices.contains((1, 1)))
      assert(collectedVertices.contains((2, 2)))
      assert(1 == filteredGraph.numEdges)
      assert(Edge(1, 2, -1) == filteredGraph.edges.collect()(0))
    }
  }

  test("aggregateNeighborValuesDirectionIn") {
    withSpark { sc =>
      // Each vertex adds to its value the value of its incoming neighbors
      val resultGraph = getGraphForAggregateNeighborValuesTest(sc).aggregateNeighborValues[Int](
        EdgeDirection.In, (nid, nvalue) => true, (vid, vvalue) => true, (vid, vvalue) => vvalue,
        (a, b) => a + b, (vid, vvalue, optAggregatedValue) => {
          (vvalue + optAggregatedValue.getOrElse(0))
        })
      assert(5 == resultGraph.numVertices)
      assert(6 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(1 == value)
          else if (vid == 1) assert(8 == value)
          else if (vid == 2) assert(3 == value)
          else if (vid == 3) assert(4 == value)
          else if (vid == 4) assert(4 == value)
      }
    }
  }

  test("aggregateNeighborValuesDirectionOut") {
    withSpark { sc =>
      // Each vertex adds to its value the value of its incoming neighbors
      val resultGraph = getGraphForAggregateNeighborValuesTest(sc).aggregateNeighborValues[Int](
        EdgeDirection.Out, (nid, nvalue) => true, (vid, vvalue) => true, (vid, vvalue) => vvalue,
        (a, b) => a + b, (vid, vvalue, optAggregatedValue) => {
          (vvalue + optAggregatedValue.getOrElse(0))
        })
      assert(5 == resultGraph.numVertices)
      assert(6 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(1 == value)
          else if (vid == 1) assert(6 == value)
          else if (vid == 2) assert(2 == value)
          else if (vid == 3) assert(4 == value)
          else if (vid == 4) assert(5 == value)
      }
    }
  }

  test("aggregateNeighborValuesDirectionEither") {
    withSpark { sc =>
      // Each vertex adds to its value the value of its incoming neighbors
      val resultGraph = getGraphForAggregateNeighborValuesTest(sc).aggregateNeighborValues[Int](
        EdgeDirection.Either, (nid, nvalue) => true, (vid, vvalue) => true, (vid, vvalue) => vvalue,
        (a, b) => a + b, (vid, vvalue, optAggregatedValue) => {
          (vvalue + optAggregatedValue.getOrElse(0))
        })
      assert(5 == resultGraph.numVertices)
      assert(6 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(2 == value)
          else if (vid == 1) assert(13 == value)
          else if (vid == 2) assert(3 == value)
          else if (vid == 3) assert(5 == value)
          else if (vid == 4) assert(5 == value)
      }
    }
  }

  test("aggregateNeighborValuesNbrPredicate") {
    withSpark { sc =>
      // Each vertex adds to its value the value of its incoming neighbors
      // filtering outgoing neighbors with id <= 2. As a result 1 should now only aggregate
      // the values of 0 and 2, and not of 3 (so instead of 6, now it should have a value of 3)
      val resultGraph = getGraphForAggregateNeighborValuesTest(sc).aggregateNeighborValues[Int](
        EdgeDirection.Out, (nid, nvalue) => (nid <= 2), (vid, vvalue) => true, (vid, vvalue) => vvalue,
        (a, b) => a + b, (vid, vvalue, optAggregatedValue) => {
          (vvalue + optAggregatedValue.getOrElse(0))
        })
      assert(5 == resultGraph.numVertices)
      assert(6 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(1 == value)
          else if (vid == 1) assert(3 == value)
          else if (vid == 2) assert(2 == value)
          else if (vid == 3) assert(4 == value)
          else if (vid == 4) assert(5 == value)
      }
    }
  }

  test("aggregateNeighborValuesVertexPredicate") {
    withSpark { sc =>
      // Each vertex adds to its value the value of its incoming neighbors
      // updating only vertices with id > 1. As a result the values of 0 and 1 should not change
      // and remain as 0 and 1, respectively.
      val resultGraph = getGraphForAggregateNeighborValuesTest(sc).aggregateNeighborValues[Int](
        EdgeDirection.Out, (nid, nvalue) => true, (vid, vvalue) => vid > 1, (vid, vvalue) => vvalue,
        (a, b) => a + b, (vid, vvalue, optAggregatedValue) => {
          (vvalue + optAggregatedValue.getOrElse(0))
        })
      assert(5 == resultGraph.numVertices)
      assert(6 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(0 == value)
          else if (vid == 1) assert(1 == value)
          else if (vid == 2) assert(2 == value)
          else if (vid == 3) assert(4 == value)
          else if (vid == 4) assert(5 == value)
      }
    }
  }

  test("aggregateNeighborValuesUpdateFunction") {
    withSpark { sc =>
      // The update function takes the minimum of the propagated value and the current value.
      // As a result when updating outgoing neighbor values, 3 and 4 should update their values
      // to 1, 0 and 1Êshould remain unchanged. and 2 should update it to 0 because the
      // we set the default aggregated value to 0.
      val resultGraph = getGraphForAggregateNeighborValuesTest(sc).aggregateNeighborValues[Int](
        EdgeDirection.Out, (nid, nvalue) => true, (vid, vvalue) => vid > 1, (vid, vvalue) => vvalue,
        (a, b) => a + b, (vid, vvalue, optAggregatedValue) => Math.min(vvalue, optAggregatedValue.getOrElse(0)))
      assert(5 == resultGraph.numVertices)
      assert(6 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(0 == value)
          else if (vid == 1) assert(1 == value)
          else if (vid == 2) assert(0 == value)
          else if (vid == 3) assert(1 == value)
          else if (vid == 4) assert(1 == value)
      }
    }
  }

  test("propagateAndAggregateDirectionIn") {
    withSpark { sc =>
      // We are propagating towards in neighbors. Therefore, when the aggregation is the min, 
      // 3 and 4 should update their values to 1 and 2, respectively, and others should remain unchanged.
      val resultGraph = getGraphForPropagateAndAggregateTest(sc).propagateAndAggregate[Int](
        EdgeDirection.In, (vid, vvals) => true, (vid, vval) => vval, (vval, eval) => vval,
        (a, b) => Math.min(a, b),
        (vid, vval, propagatedVal) => Math.min(vval, propagatedVal))
      assert(5 == resultGraph.numVertices)
      assert(4 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(0 == value)
          else if (vid == 1) assert(1 == value)
          else if (vid == 2) assert(2 == value)
          else if (vid == 3) assert(1 == value)
          else if (vid == 4) assert(2 == value)
      }
    }
  }

  test("propagateAndAggregateDirectionOut") {
       withSpark { sc =>
        // We are propagating towards out neighbors. Therefore, when the aggregation is the max, 
        // 1 and 2 should update their values to 3 and 4, respectively, and others should remain unchanged.
        val resultGraph = getGraphForPropagateAndAggregateTest(sc).propagateAndAggregate[Int](
          EdgeDirection.Out, (vid, vvals) => true, (vid, vval) => vval, (vval, eval) => vval,
          (a, b) => Math.max(a, b),
          (vid, vval, propagatedVal) => Math.max(vval, propagatedVal))
        assert(5 == resultGraph.numVertices)
        assert(4 == resultGraph.numEdges)
        resultGraph.vertices.collect.foreach {
          case (vid, value) =>
            if (vid == 0) assert(0 == value)
            else if (vid == 1) assert(3 == value)
            else if (vid == 2) assert(4 == value)
            else if (vid == 3) assert(3 == value)
            else if (vid == 4) assert(4 == value)
        }
      }
    }
  
    test("propagateAndAggregateDirectionEither") {
       withSpark { sc =>
        // We are propagating towards all neighbors. Therefore, when the aggregation is the max, 
        // every vertex value should be 4 in the end.
        val resultGraph = getGraphForPropagateAndAggregateTest(sc).propagateAndAggregate[Int](
          EdgeDirection.Either, (vid, vvals) => true, (vid, vval) => vval, (vval, eval) => vval,
          (a, b) => Math.max(a, b),
          (vid, vval, propagatedVal) => Math.max(vval, propagatedVal))
        assert(5 == resultGraph.numVertices)
        assert(4 == resultGraph.numEdges)
        resultGraph.vertices.collect.foreach { case (vid, value) => assert(4 == value) }
      }
    }
  
    test("propagateAndAggregateStartVertexPredicate") {
       withSpark { sc =>
        // We are propagating towards out neighbors, starting from vertex 3. Therefore, when the aggregation is the max, 
        // 1 should update its value to 3 and others should remain unchanged.
        val resultGraph = getGraphForPropagateAndAggregateTest(sc).propagateAndAggregate[Int](
          EdgeDirection.Out,
          (vid, vvals) => (vid == 3) /* only start from vertex 3 */,
          (vid, vval) => vval, (vval, eval) => vval, (a, b) => Math.max(a, b),
          (vid, vval, propagatedVal) => Math.max(vval, propagatedVal))
        assert(5 == resultGraph.numVertices)
        assert(4 == resultGraph.numEdges)
        resultGraph.vertices.collect.foreach {
          case (vid, value) =>
            if (vid == 0) assert(0 == value)
            else if (vid == 1) assert(3 == value)
            else if (vid == 2) assert(2 == value)
            else if (vid == 3) assert(3 == value)
            else if (vid == 4) assert(4 == value)
        }
      }
    }
    

  test("propagateAndAggregateDirectionPropagateWithEdgeDistances") {
    withSpark { sc =>
      // We are running single source shortest paths from vertex 0. The graph is as follows:
      // 0 to 4 form a cycle. There is also one edge between 0 and 5. (0 -> 4) edge has value 10,
      // every other edge has value 1.
      // When we propagate the distances by including the edge values, we expect the result distances to be: 
      // 1 has distance 1, 2 has distance 2, 3 has distance 3, 4 has distance 4 and 5 has distance 1
      val edges = sc.parallelize((1 to 1).flatMap(x => {
        Vector(Edge(0, 1, 1), Edge(0, 4, 10), Edge(1, 2, 1), Edge(2, 3, 1), Edge(3, 4, 1), Edge(0, 5, 1))
      }))
      // Vertex 0 has value 0, others have value Int.MaxValue
      val vertices = sc.parallelize((0 to 5).map(vid =>
        if (vid == 0) (vid: VertexId, 0) else (vid: VertexId, Int.MaxValue)))
      val graph = Graph(vertices, edges).cache()
      val resultGraph = graph.propagateAndAggregate[Int](
        EdgeDirection.Either,
        (vid, vvals) => (vid == 0) /* only start from vertex 0 */ ,
        (vid, vval) => vval, (vval, eval) => vval + eval, (a, b) => Math.min(a, b),
        (vid, vval, propagatedVal) => Math.min(vval, propagatedVal))
      assert(6 == resultGraph.numVertices)
      assert(6 == resultGraph.numEdges)
      resultGraph.vertices.collect.foreach {
        case (vid, value) =>
          if (vid == 0) assert(0 == value)
          else if (vid == 1) assert(1 == value)
          else if (vid == 2) assert(2 == value)
          else if (vid == 3) assert(3 == value)
          else if (vid == 4) assert(4 == value)
          else if (vid == 5) assert(1 == value)
      }
    }
  }

  private def getGraphForPropagateAndAggregateTest(sc: SparkContext): Graph[Int, Int] = {
    // 5 vertices that form a tree. 0 is the root pointing at 3 and 4. 1 is 3's child and
    // 2 is 4's child. As a result, there is a total of 4 edges.
    val edges = sc.parallelize((1 to 1).flatMap(x => {
      Vector(Edge(0, 3, -1), Edge(0, 4, -1), Edge(3, 1, -1), Edge(4, 2, -1))
    }))
    // Vertex i have value i
    val vertices = sc.parallelize((0 to 4).map(vid => (vid: VertexId, vid)))
    Graph(vertices, edges).cache()
  }

  private def getGraphForAggregateNeighborValuesTest(sc: SparkContext): Graph[Int, Int] = {
    // 5 vertices, 1 points at every vertex except 4. 0, 3, and 4 point back to 1.
    // As a result 2 is a vertex without outgoing edges, and 4 is a vertex without incoming edges
    val edges = sc.parallelize((1 to 1).flatMap(x => {
      Vector(Edge(1, 0, -1), Edge(1, 2, -1), Edge(1, 3, -1), Edge(0, 1, -1), Edge(3, 1, -1), Edge(4, 1, -1))
    }))
    // Vertex i have value i
    val vertices = sc.parallelize((0 to 4).map(vid => (vid: VertexId, vid)))
    Graph(vertices, edges).cache()
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
