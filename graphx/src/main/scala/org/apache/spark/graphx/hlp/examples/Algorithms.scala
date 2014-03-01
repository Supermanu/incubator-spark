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
package org.apache.spark.graphx.hlp.examples

import scala.collection.JavaConversions._
import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

object Algorithms {

  def main(args: Array[String]) {
    val algName = args(0);
    val verticesFileName = args(1);
    val edgesFileName = args(2);
    val sc = new SparkContext("local", algName)
    algName match {
      case "wcc" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => -1L, (srcDstId, evalsString) => -1).cache()
        weaklyConnectedComponents(g)
      }
      case "sssp" => {
        val srcId = args(3).toLong
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => Integer.MAX_VALUE,
          (srcDstId, evalsString) => evalsString(0).toInt).cache()
        singleSourceShortestPaths(g, srcId)
      }
      case "pr" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => -1.0, (srcDstId, evalsString) => -1).cache()
        pageRank(g)
      }
      case "hits" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => (1.0, 1.0), (srcDstId, evalsString) => -1).cache()
        hits(g)
      }
      case "scc" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => (-1L, false), (srcDstId, evalsString) => -1).cache()
        stronglyConnectedComponents(g)
      }
      case "mst" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => new MSTVertexValue(-1, -1, -1, -1.0), (srcDstId, evalsString) =>
            new MSTEdgeValue(evalsString(0).toDouble, srcDstId._1, srcDstId._2)).cache()
        boruvkasMinimumSpanningTree(g)
      }
      case "amwm" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => -1L, (srcDstId, evalsString) => evalsString(0).toDouble)
        approximateMaxWeightMatching(g)
      }
      case "gc" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => (id, MISType.Unknown: MISType), (srcDstId, evalsString) => -1)
        graphColoringBasedOnMaximalIndependentSet(g)
      }
      case "conductance" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => ((id % 2) == 0, 0), (srcDstId, evalsString) => -1)
        conductance(g)
      }
      case "rmbm" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => (-1L, -1L), (srcDstId, evalsString) => -1)
        randomMaximalBipartiteMatching(g)
      }
      case "bc" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => new BCVertexValue(-1, 0.0, 0.0, 0.0),
          (srcDstId, evalsString) => -1)
        betweennessCentrality(g)
      }
      case "semiclustering" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => Nil: List[(Double, Double, List[VertexId])],
          (srcDstId, evalsString) => evalsString(0).toDouble)
        semiClustering(g)
      }
      case "doublefringe" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => -1, (srcDstId, evalsString) => -1)
        doubleFringeDiameterEstimation(g)
      }
      case "kcore" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => -1L, (srcDstId, evalsString) => -1)
        kCore(g)
      }
      case "triangles" => {
        triangleFindingPerNode(sc, edgesFileName)
      }
      case "ktruss" => {
        kTruss(sc, edgesFileName)
      }
      case "metis" => {
        var g = GraphLoader.textFileWithVertexValues(sc, verticesFileName, edgesFileName,
          (id, values) => new MetisVertexValue(1.0, -1, -1, -1),
          (srcDstId, evalsString) => evalsString(0).toDouble)
        simpleMETIS(sc, g)
      }
    }
    sc.stop()
  }

  private def simpleMETIS(sc: SparkContext, graph: Graph[MetisVertexValue, Double]) = {
    var g = graph
    val numPartitions = 2
    val thresholdNumEdgesForInitialPartitioning = 3
    // Step 1: Coarsening
    var coarsenedGraphs: ListBuffer[Graph[MetisVertexValue, Double]] = new ListBuffer()
    while (g.numEdges > thresholdNumEdgesForInitialPartitioning) {
      val graphWithSuperVertexIDsAndCoarsenedGraph = coarsenGraph(g)
      coarsenedGraphs.append(graphWithSuperVertexIDsAndCoarsenedGraph._1)
      println("coarsened graph. numEdges: " + graphWithSuperVertexIDsAndCoarsenedGraph._2.numEdges)
      g = graphWithSuperVertexIDsAndCoarsenedGraph._2
    }
    // Step 2: Initial Partitioning
    var superVertexIDPartitionID = doInitialPartitioning(sc, g, numPartitions)
    debugRDD(superVertexIDPartitionID.collect, "initial partitioning results")
    // Step 3: Uncoarsen and refine
    var finalGraph: Graph[MetisVertexValue, Double] = null
    for (i <- coarsenedGraphs.length - 1 to 0 by -1) {
      println("i: " + i)
      val coarsenedGraph = coarsenedGraphs(i)
      debugRDD(coarsenedGraph.vertices.collect, "coarsened graph vertices i: " + i)
      val superVertexIDVertexRDD = coarsenedGraph.vertices.map(v => (v._2.superVertexID, v))
      val superVertexIDVertexPartitionIDRDD = superVertexIDVertexRDD.join(superVertexIDPartitionID)
      val newVertices = superVertexIDVertexPartitionIDRDD.map {
        superVertexIDVertexPartitionID =>
          {
            superVertexIDVertexPartitionID._2._1._2.partitionID =
              superVertexIDVertexPartitionID._2._2
            superVertexIDVertexPartitionID._2._1
          }
      }
      debugRDD(newVertices.collect, "new vertices iteration: " + i)
      var uncoarsenedGraph: Graph[MetisVertexValue, Double] = Graph(newVertices,
        coarsenedGraph.edges)
      superVertexIDPartitionID = uncoarsenedGraph.vertices.map { v => (v._1, v._2.partitionID) }
      if (i == 0) {
        finalGraph = uncoarsenedGraph
      }
    }

    debugRDD(finalGraph.vertices.collect, "final vertices")
    debugRDD(finalGraph.edges.collect, "final edges")

    // POSTPROCESSING
    val totalWeightAcrossPartitions = finalGraph.triplets.map(e =>
      if (e.srcAttr.partitionID != e.dstAttr.partitionID) { e.attr } else { 0.0 }).reduce(_ + _)
    println("totalWeightOfEdgesCrossingPartitions: " + totalWeightAcrossPartitions)
  }

  private def doInitialPartitioning(sc: SparkContext, g: Graph[MetisVertexValue, Double],
    numPartitions: Int): RDD[(VertexId, Int)] = {
    // TODO(semih): There needs to be a sequential algorithm ran here! This is for demonstration
    // purposes for metis_test_vertices/edges.txt files.
    val tmpSupervertexIDPartitionID: Array[(VertexId, Int)] = Array((3L, 1), (7L, 2))
    sc.parallelize(tmpSupervertexIDPartitionID, 4)
  }

  private def coarsenGraph(g: Graph[MetisVertexValue, Double]): (Graph[MetisVertexValue, Double], 
    Graph[MetisVertexValue, Double]) = {
    var h = g.updateVerticesUsingLocalEdges(
      EdgeDirection.Out,
      (vid, vdata, edgesList) => {
        var pickedNbrId = vid
        var pickedNbrWeight = Double.MinValue
        if (!edgesList.isEmpty) {
          for (edge <- edgesList) {
            if (edge.attr > pickedNbrWeight ||
              (edge.attr == pickedNbrWeight && edge.dstId > pickedNbrId)) {
              pickedNbrWeight = edge.attr
              pickedNbrId = edge.dstId
            }
          }
        }
        vdata.superVertexID = pickedNbrId
        vdata
      })
    // Find the roots of conjoined trees
    h = h.updateSelfUsingAnotherVertex[VertexId](
      (vid, vdata) => true,
      (vid, vdata) => vdata.superVertexID,
      (othervid, othervdata) => othervdata.superVertexID,
      (vid, vdata, msg) => {
        if (msg == vid && vid > vdata.superVertexID) {
          vdata.superVertexID = vid
        }
        vdata
      })
    // find supervertex ID
    h = pointerJumping(h, (vid, vdata) => vdata.superVertexID,
      { (vid, vdata, parentsParent) => vdata.superVertexID = parentsParent; vdata })
    // supervertexIdFieldF: (VertexId, VD) => VertexId, vertexAggrF: Seq[VD] => VD,
    // edgeAggrF: Seq[ED] => ED
    val coarsenedGraph = h.formSupervertices(
      (vid, vdata) => vdata.superVertexID,
      vertexValues => {
        val superVertexID = vertexValues(0).superVertexID
        var totalWeight = vertexValues(0).weight
        for (index <- 1 to (vertexValues.length - 1)) {
          totalWeight += vertexValues(index).weight
        }
        new MetisVertexValue(totalWeight, superVertexID, -1, -1)
      },
      edges => {
        var totalWeight = edges(0)
        for (index <- 1 to (edges.length - 1)) {
          totalWeight += edges(index)
        }
        totalWeight
      })
    (h, coarsenedGraph)
  }

  private def randomMaximalBipartiteMatching(graph: Graph[(VertexId, VertexId), Int]) = {
    var g = graph
    var iterNo = 1
    while (g.numEdges > 0) {
      println("starting iterNo: " + iterNo)
      iterNo += 1
      g = g.updateVerticesUsingLocalEdges(
        EdgeDirection.In,
        (vid, vdata, edges) => {
          if ((vid % 2) == 1) vdata
          else {
            var tmpPick = vdata._1
            var actualPick = vdata._2
            if (edges.isEmpty) actualPick = -2;
            else {
              var minEdgeId = edges(0).srcId
              for (index <- 1 to (edges.length - 1)) {
                if (edges(index).srcId < minEdgeId) {
                  minEdgeId = edges(index).srcId
                }
              }
              tmpPick = minEdgeId
            }
            (tmpPick, actualPick)
          }
        })
      debugRDD(g.vertices.collect(), "vertices after right vertices picking a tmpPick (with " +
        "min ID). iterNo: " + iterNo)
      g = g.updateAnotherVertexUsingSelf[VertexId](
        (vid, vdata) => vid % 2 == 0,
        (vid, vdata) => vdata._1,
        (vid, vdata) => vid,
        msgs => {
          var minEdgeId = msgs(0)
          for (index <- 1 to (msgs.length - 1)) {
            if (msgs(index) < minEdgeId) {
              minEdgeId = msgs(index)
            }
          }
          minEdgeId
        },
        (vid, vdata, aggrMsg) => (vdata._1, aggrMsg))
      debugRDD(g.vertices.collect(), "vertices after left vertices pick an actualPick " +
        " (with min ID). iterNo: " + iterNo)
      g = g.updateAnotherVertexUsingSelf[VertexId](
        (vid, vdata) => vid % 2 == 1 && vdata._2 >= 0,
        (vid, vdata) => vdata._2,
        (vid, vdata) => vid,
        msgs => {
          assert(1 == msgs.length, "there should be only one message if there are msgs. iterNo: "
            + iterNo)
          msgs(0)
        },
        (vid, vdata, aggrMsg) => (vdata._1, aggrMsg))
      debugRDD(g.vertices.collect(), "vertices after right vertices are notified if they're " +
        "picked: iterNo: " + iterNo)
      outputMatchedVertices(g.vertices.filter(v => v._2._2 > 0), iterNo)
      g = g.filterVertices((vid, vdata) => vdata._2 == -1)
    }
  }

  private def kTruss(sc: SparkContext, edgesFileName: String) = {
    val k = 4
    var edges = loadEdges(sc, edgesFileName)
    debugRDD(edges.collect, "starting edges")
    var previousNumEdges = Long.MaxValue
    var numEdges = edges.count
    var iterationNo = 1
    while (previousNumEdges != numEdges) {
      previousNumEdges = numEdges
      val triangles = findTriangles(edges)
      val numTrianglesPerEdge = triangles.flatMap(
        triangle => List((triangle._1, 1), ((triangle._1._2, triangle._1._1), 1),
          ((triangle._1._1, triangle._2._1), 1), ((triangle._2._1, triangle._1._1), 1),
          ((triangle._1._2, triangle._2._1), 1), ((triangle._2._1, triangle._1._2), 1)))
        .reduceByKey(_ + _)
      debugRDD(numTrianglesPerEdge.collect, "num triangles per edge")
      edges = numTrianglesPerEdge.filter(edgeNumTriangles => edgeNumTriangles._2 >= (k - 2))
        .map(edgeNumTriangles => edgeNumTriangles._1)
      debugRDD(edges.collect, "new edges for iterationNo: " + iterationNo)
      iterationNo += 1
      numEdges = edges.count
      println("previousNumEdges: " + previousNumEdges + " newNumEdges: " + numEdges)
    }
    // POSTPROCESSING
    sc.stop()
  }

  private def triangleFindingPerNode(sc: SparkContext, edgesFileName: String) = {
    val triangles = findTriangles(loadEdges(sc, edgesFileName))
    val numTrianglesPerVertex = triangles.flatMap(
      triangle => List((triangle._1._1, 1), (triangle._1._2, 1), (triangle._2._1, 1)))
      .reduceByKey(_ + _)
    debugRDD(numTrianglesPerVertex.collect, "num triangles per vertex")
    // POSTPROCESSING
    sc.stop()
  }

  private def findTriangles(edges: RDD[(Int, Int)]): RDD[((Int, Int), (Int, Int))] = {
    val edgesAsKey = edges.map(v => (v, 1))
    val edgesGroupedByVertices = edges.groupByKey()
    val missingEdges = edgesGroupedByVertices.flatMap(vertexIDNbrIDs =>
      {
        val vertexID = vertexIDNbrIDs._1
        val nbrIDs = vertexIDNbrIDs._2
        var listOfMissingEdges: ListBuffer[((Int, Int), Int)] = ListBuffer()
        for (i <- 0 to nbrIDs.length - 1) {
          if (vertexID > nbrIDs(i)) {
            for (j <- (i + 1) to nbrIDs.length - 1) {
              if (vertexID > nbrIDs(j)) {
                listOfMissingEdges.add(((nbrIDs(i), nbrIDs(j)), vertexID))
              }
            }
          }
        }
        listOfMissingEdges
      })
    val triangles = missingEdges.join(edgesAsKey)
    println("num total triangles: " + triangles.count)
    triangles
  }

  private def loadEdges(sc: SparkContext, edgesFileName: String): RDD[(Int, Int)] = {
    sc.textFile(edgesFileName).flatMap(line =>
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        if (lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        Some((lineArray(0).trim.toInt, lineArray(1).trim.toInt))
      } else { None })
  }

  private def kCore(graph: Graph[VertexId, Int]) = {
    var g = graph
    val k = 3
    val maxIterationNo = 10
    var previousNumVertices = Long.MaxValue
    var currentNumVertices = g.numVertices
    var iterationNo = 1
    while (previousNumVertices != currentNumVertices && iterationNo < maxIterationNo) {
      println("iterationNo: " + iterationNo)
      previousNumVertices = currentNumVertices
      g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out,
        (vid, vdata, edges) => edges.size >= k)
      debugRDD(g.vertices.collect, "vertices after filtering for the " + iterationNo + "th time")
      currentNumVertices = g.numVertices
      iterationNo += 1
    }

    // POSTPROCESSING
    g = weaklyConnectedComponents(g)

    var maxComponentIDAndSize = g.vertices.map { v => (v._2, 1) }.reduceByKey(_ + _).reduce(
      (firstComp, secondComp) => {
        if (firstComp._2 > secondComp._2) { firstComp }
        else { secondComp }
      })
    println("maxComponentID: " + maxComponentIDAndSize._1 + " componentSize: "
      + maxComponentIDAndSize._2)
    g = g.filterVertices((vid, vdata) => vdata == maxComponentIDAndSize._1)
    println("largest kCore: num vertices: " + g.numVertices + " numEdges: " + g.numEdges)
  }

  private def doubleFringeDiameterEstimation(graph: Graph[Int, Int]) = {
    var g = graph
    var lowerBound = Int.MinValue
    var upperBound = Int.MaxValue
    var iterationNo = 1
    val maxIterationNo = 3
    val maxVerticesToPickFromFringe = 5
    while (lowerBound < upperBound && iterationNo <= maxIterationNo) {
      val r = g.pickRandomVertex()
      println("picked r: " + r)
      val (h, lengthOfTreeFromR) = runBfsFromVertexAndCountTheLengthOfTree(g, r)
      g = h
      println("lengthOfTreeFromR: " + r + " is " + lengthOfTreeFromR)
      // Pick a random vertex from the leaf. Implements a random picking in a streaming fashion
      val a = g.pickRandomVerticesWithPredicate((vid, vdata) => vdata == lengthOfTreeFromR, 1)(0)
      println("picked a: " + a)
      val (t, lengthOfTreeFromA) = runBfsFromVertexAndCountTheLengthOfTree(g, a)
      g = t
      lowerBound = scala.math.max(lowerBound, lengthOfTreeFromA)
      val halfWay = lengthOfTreeFromA / 2
      println("lowerBound: " + lowerBound + " halfway: " + halfWay)
      // pick a vertex that's in the middle
      val u = g.pickRandomVerticesWithPredicate((vid, vdata) => vdata == halfWay, 1)(0)
      println("picked u: " + u)
      val (k, lengthOfTreeFromU) = runBfsFromVertexAndCountTheLengthOfTree(g, u)
      g = k
      println("lengthOfTreeFromC: " + lengthOfTreeFromU)
      val fringeOfU = g.pickRandomVerticesWithPredicate((vid, vdata) => vdata == lengthOfTreeFromU,
        maxVerticesToPickFromFringe)
      println("fringeOfC: " + fringeOfU)
      var maxDistance = -1
      for (src <- fringeOfU) {
        println("src: " + src)
        val (tmpG, lengthOfTreeFromSrc) = runBfsFromVertexAndCountTheLengthOfTree(g, src)
        if (lengthOfTreeFromSrc > maxDistance) {
          maxDistance = lengthOfTreeFromSrc
        }
      }
      println("maxDistance: " + maxDistance + " fringOfU.size: " + fringeOfU.size)
      val twiceEccentricityMinus1 = ((2 * lengthOfTreeFromU) - 1)
      println("twiceEccentricityMinus1: " + twiceEccentricityMinus1)
      if (fringeOfU.size > 1) {
        if (maxDistance == twiceEccentricityMinus1) {
          upperBound = twiceEccentricityMinus1
          println("found the EXACT diameter: " + upperBound)
          assert(lowerBound == upperBound)
        } else if (maxDistance < twiceEccentricityMinus1) {
          upperBound = twiceEccentricityMinus1 - 1
        } else if (maxDistance == (twiceEccentricityMinus1 + 1)) {
          upperBound = lengthOfTreeFromA
          println("found the EXACT diameter with multiple fringe nodes but maxDistance matches " +
            "lengthOfTreeFromA. diameter: " + upperBound)
          assert(lengthOfTreeFromA == (twiceEccentricityMinus1 + 1))
          assert(lowerBound == upperBound)
        } else {
          println("ERROR maxDistance: " + maxDistance + " CANNOT be  > than twiceEccentricity: "
            + (twiceEccentricityMinus1 + 1))
          sys.exit(-1)
        }
      } else {
        // When there is a single vertex in the fringe of C or B(u) == twiceEccentricityMinus1
        // we find the exact diameter, which is equal to the diameter of T_u=lengthOfTreeFromA 
        upperBound = lengthOfTreeFromA
        println("found the EXACT diameter with single fringe node. diameter: " + upperBound)
        assert(lowerBound == upperBound)
      }
      upperBound = scala.math.min(upperBound, maxDistance)
      println("finished iterationNo: " + iterationNo + " lowerBound: " + lowerBound +
        " upperBound: " + upperBound)
      iterationNo += 1
    }
    // POSTPROCESSING
    println("final lower bound:" + lowerBound + " final upperBound: " + upperBound)
  }

  private def runBfsFromVertexAndCountTheLengthOfTree(g: Graph[Int, Int], src: VertexId): 
    (Graph[Int, Int], Int) = {
    var h = g.updateVertices((vid, vdata) => true,
      (vid, vdata) => if (vid == src) 0 else Int.MaxValue)
    debugRDD(h.vertices.collect, "vertices after initializing distances")
    //updateF: (VertexId, VD, U) => VD
    h = h.propagateAndAggregate[Int](
      EdgeDirection.Out,
      (vid, vdata) => vid == src,
      (vid, vdata) => vdata,
      (vval, edata, nbrdata) => vval + 1,
      Math.min,
      (vid, vdata, aggrMsg) => Math.min(vdata, aggrMsg))
    debugRDD(h.vertices.collect, "vertices after propagating from src: " + src)
    val lengthOfTreeFromSrc = h.aggregateGlobalValue[Int](
      vidVdata => if (vidVdata._2 < Int.MaxValue) { vidVdata._2 } else { Int.MinValue }, Math.max)
    (h, lengthOfTreeFromSrc)
  }

  private def semiClustering(graph: Graph[List[(Double, Double, List[VertexId])], Double]) = {
    var g = graph
    g = g.updateVerticesUsingLocalEdges(EdgeDirection.In,
      (vid, vdata, edges) => {
        val weightEdges = sumTotalWeight(edges)
        List((0.0, weightEdges, List(vid)))
      })
    debugRDD(g.vertices.collect(), "vertices after initializing semicluster values")
    for (i <- 1 to 3) {
      g = g.aggregateNeighborValuesUsingLocalEdges[List[List[(Double, Double, List[VertexId])]]](
        EdgeDirection.Out /* local edges direction */ ,
        EdgeDirection.Out /* nbr to aggregate direction */ ,
        (nbrid, nbrdata) => true,
        (vid, vdata) => true,
        (vid, vdata) => List(vdata),
        (list1, list2) => list1 ::: list2,
        (vid, vdata, edges, optMsgs) => aggregateSemiClusters((vid, vdata, edges, optMsgs)))
      debugRDD(g.vertices.collect(), "vertices after running one iteration of aggregation. " +
        " iterationNo: " + i)
    }
    // POSTPROCESSING
    val maxClusterAndScore = g.aggregateGlobalValue[(Double, List[VertexId])](
      vidVdata => {
        val semiClusterAndScores = computeAndSortExtendedSemiClusters(vidVdata._2)
        if (semiClusterAndScores.isEmpty) { (Int.MinValue, Nil) }
        else { (semiClusterAndScores(0)._1, semiClusterAndScores(0)._2._3) }
      },
      (a, b) => { if (a._1 > b._1) a else b })
    println("maxClusterAndScore: " + maxClusterAndScore)
  }

  def sumTotalWeight(edges: Seq[Edge[Double]]): Double = {
    var sum = 0.0
    for (edge <- edges) { sum += edge.attr }
    sum
  }

  def aggregateSemiClusters(vidVdataEdgesMsgs: (VertexId, List[(Double, Double, List[VertexId])], 
    Array[Edge[Double]], Option[List[List[(Double, Double, List[VertexId])]]])): 
    List[(Double, Double, List[VertexId])] = {
    val maxClustersToReturn = 3
    val vid = vidVdataEdgesMsgs._1
    val vdata = vidVdataEdgesMsgs._2
    val nbrs = vidVdataEdgesMsgs._3
    var allSemiClusters = vdata
    println("vertex ID: " + vid + " previousSemiClusters: " + allSemiClusters)
    val nbrSemiClusterLists = vidVdataEdgesMsgs._4
    if (!nbrSemiClusterLists.isDefined) {
      allSemiClusters
    }

    for (nbrSemiClusterList <- nbrSemiClusterLists.get) {
      for (nbrSemiCluster <- nbrSemiClusterList) {
        if (!nbrSemiCluster._3.contains(vid)) {
          val weightInAndOutCluster = countWeightInAndOutCluster(nbrs, nbrSemiCluster._3)
          val weightsInCluster = nbrSemiCluster._1 + weightInAndOutCluster._1
          // The weights outside the cluster is equal to:
          // previousWeightOutCluster MINUS
          // inside weigths now in the cluster due to current vertex PLUS
          // outside weights of the current vertex
          val weightOutCluster =
            nbrSemiCluster._2 - weightInAndOutCluster._1 + weightInAndOutCluster._2
          println("cluster: " + (vid :: nbrSemiCluster._3) + " weightsInCluster: "
            + weightsInCluster + " weightOutCluster: " + weightOutCluster)
          allSemiClusters =
            (weightsInCluster, weightOutCluster, vid :: nbrSemiCluster._3) :: allSemiClusters
        }
      }
    }
    println("vertex ID: " + vid + " allSemiClusters: " + allSemiClusters)
    val finalExtendedSemiClusters = computeAndSortExtendedSemiClusters(allSemiClusters).slice(0,
      maxClustersToReturn)
    println("vertex ID: " + vid + " sortedFinalExtendedSemiClusters: " + finalExtendedSemiClusters)
    var finalSemiClusters: List[(Double, Double, List[VertexId])] = Nil
    for (finalExtendedSemiCluster <- finalExtendedSemiClusters) {
      finalSemiClusters = finalExtendedSemiCluster._2 :: finalSemiClusters
    }
    finalSemiClusters
  }

  private def computeAndSortExtendedSemiClusters(
    allSemiClusters: List[(Double, Double, List[VertexId])]): 
    List[(Double, (Double, Double, List[VertexId]))] = {
    val boundaryEdgeScore = 1.0
    var extendedSemiClusters: List[(Double, (Double, Double, List[VertexId]))] = Nil
    for (semiCluster <- allSemiClusters) {
      val numVerticesInCluster = semiCluster._3.length
      val score = (semiCluster._1 - boundaryEdgeScore * semiCluster._2) /
        (numVerticesInCluster * math.max(1, (numVerticesInCluster - 1)) / 2.0)
      extendedSemiClusters = (score, semiCluster) :: extendedSemiClusters
    }
    extendedSemiClusters.sortWith((firstCluster, secondCluster) =>
      firstCluster._1 > secondCluster._1)
  }

  private def countWeightInAndOutCluster(nbrs: Array[Edge[Double]],
    verticesInCluster: List[VertexId]): (Double, Double) = {
    println("nbrs: " + nbrs)
    println("verticesInCluster: " + verticesInCluster)
    var weightInCluster = 0.0;
    var weightOutCluster = 0.0
    for (nbr <- nbrs) {
      if (verticesInCluster.contains(nbr.dstId)) { weightInCluster += nbr.attr }
      else { weightOutCluster += nbr.attr }
    }
    (weightInCluster, weightOutCluster)
  }

  private def betweennessCentrality(graph: Graph[BCVertexValue, Int]) = {
    var g = graph
    for (i <- 1 until 5) {
      println("starting iteration: " + i)
      val s = g.pickRandomVertex()
      println("random vertex: " + s)
      g = g.updateVertices((vid, vdata) => true, (vid, vdata) => {
        if (vid == s) {
          vdata.bfsLevel = 0; vdata.sigma = 1.0; vdata.delta = 0.0
        } else {
          vdata.bfsLevel = Int.MaxValue; vdata.sigma = 0.0; vdata.delta = 0.0
        }
        vdata
      })
      debugRDD(g.vertices.collect(), "vertices after initializing values")
      g = g.propagateAndAggregate[Int](
        EdgeDirection.Out,
        (vid, vdata) => vid == s,
        (vid, vdata) => vdata.bfsLevel,
        (msg, edata, nbrdata) => msg + 1,
        Math.min,
        (vid, vdata, aggrMsg) => {
          println("vid: " + vid + " with level: " + vdata.bfsLevel + " got message: " + aggrMsg)
          if (aggrMsg < vdata.bfsLevel) {
            val newVdata = vdata.clone()
            newVdata.bfsLevel = Math.min(newVdata.bfsLevel, aggrMsg); newVdata
          } else vdata
        })

      val maxBfsLevel = g.aggregateGlobalValue[Int](
        vidVdata => {
          if (vidVdata._2.bfsLevel < Int.MaxValue) vidVdata._2.bfsLevel else Int.MinValue
        },
        Math.max)
      println("maxBfsLevel: " + maxBfsLevel)
      debugRDD(g.vertices.collect(), "vertices before updating sigmas")
      for (j <- 1 to maxBfsLevel) {
        println("starting sigma updating level: " + j)
        //aggregateF, updateF
        g = g.aggregateNeighborValues[Double](
          EdgeDirection.In,
          (nbrid, nbrdata) => nbrdata.bfsLevel == j - 1,
          (vid, vdata) => vdata.bfsLevel == j,
          (nbrid, nbrdata) => nbrdata.sigma,
          _ + _,
          (vid, vdata, optAggrMsg) => {
            if (optAggrMsg.isDefined) {
              val newVdata = vdata.clone
              newVdata.sigma = optAggrMsg.get;
              newVdata
            } else vdata
          })
        debugRDD(g.vertices.collect(), "vertices after updating sigmas of level " + j)
      }

      for (j <- (maxBfsLevel - 1) to 0 by -1) {
        g = g.aggregateNeighborValues[Double](
          EdgeDirection.Out,
          (nbrid, nbrdata) => nbrdata.bfsLevel == j + 1,
          (vid, vdata) => vdata.bfsLevel == j,
          (nbrid, nbrdata) => (1 + nbrdata.delta) / nbrdata.sigma,
          _ + _,
          (vid, vdata, optAggrMsg) => {
            if (optAggrMsg.isDefined) {
              val newVdata = vdata.clone()
              newVdata.delta = newVdata.sigma * optAggrMsg.get
              newVdata.bc += newVdata.delta;
              newVdata
            } else vdata
          })
        debugRDD(g.vertices.collect(), "vertices after updating deltas and bc of level " + j)
      }
    }

    // POSTPROCESSING
    val maxBCVertexAndBC = g.aggregateGlobalValue[(VertexId, Double)](
      vidVdata => (vidVdata._1, vidVdata._2.bc),
      (a, b) => if (a._2 > b._2) a else b)
    println("maxBCVertex: " + maxBCVertexAndBC._1 + " bc: " + maxBCVertexAndBC._2)
  }

  private def conductance(graph: Graph[(Boolean, Int), Int]) = {
    var g = graph
    val degreeOfVerticesInSubset = g.aggregateGlobalValueWithLocalEdges[Int](EdgeDirection.Out,
      vidVdataEdges => if (vidVdataEdges._2._1._1) vidVdataEdges._2._2.size else 0, _ + _)
    println("degreeOfVerticesInSubset: " + degreeOfVerticesInSubset)
    val degreeOfVerticesNotInSubset = g.aggregateGlobalValueWithLocalEdges[Int](EdgeDirection.Out,
      vidVdataEdges => if (!vidVdataEdges._2._1._1) vidVdataEdges._2._2.size else 0, _ + _)
    println("degreeOfVerticesNotInSubset: " + degreeOfVerticesNotInSubset)
    //  updateF
    g = g.aggregateNeighborValues[Int](
      EdgeDirection.Out,
      (nbrId, nbrdata) => nbrdata._1,
      (vId, vdata) => !vdata._1,
      (vid, vdata) => 1,
      _ + _,
      (vid, vdata, optAggrMsg) => if (optAggrMsg.isDefined) (vdata._1, optAggrMsg.get) else vdata)
    val edgesCrossing = g.aggregateGlobalValue[Int]((vidVdata) => vidVdata._2._2, _ + _)
    println("edgesCrossing: " + edgesCrossing)

    // POSTPROCESSING
    if (degreeOfVerticesInSubset == 0 && degreeOfVerticesNotInSubset == 0) {
      if (edgesCrossing == 0) { println("conductance is 0") }
      else { println("conductance is INFINITY") }
    } else {
      val conductance = edgesCrossing.doubleValue / Math.max(degreeOfVerticesInSubset,
        degreeOfVerticesNotInSubset).doubleValue()
      println("conductance is " + conductance)
    }
  }

  private def graphColoringBasedOnMaximalIndependentSet(graph: Graph[(VertexId, MISType), Int]) = {
    var g = graph
    var colorID = 0
    while (g.numVertices > 0) {
      println("g.numVertices: " + g.numVertices)
      colorID += 1
      val oldGraph = g
      var iterNo = 0
      // Initialize vertices to Unknown
      g = g.updateVertices((vid, vdata) => true, (vid, vdata) => (vdata._1, MISType.Unknown))
      var numUnknownVertices = g.numVertices
      while (numUnknownVertices > 0) {
        println("numUnknownVertices: " + numUnknownVertices)
        println("numEdges: " + g.numEdges)
        println("oldnumEdges: " + oldGraph.numEdges)
        println("oldnumVertices: " + oldGraph.numVertices)
        println("putting randomly inset...")
        g = g.updateVerticesUsingLocalEdges(
          EdgeDirection.Out,
          (vid, vdata, edges) => {
            println("running update vertices...")
            var mistype = vdata._2
            if (MISType.Unknown == vdata._2) {
              if (edges.isEmpty) {
                mistype = MISType.InSet
              } else {
                val prob = 1.0 / (2.0 * edges.size)
                val nextDouble = {
                  if (iterNo == 1) {
                    new Random(new Random(vid).nextInt).nextDouble
                  } else {
                    new Random().nextDouble
                  }
                }
                println("prob: " + prob + " random: " + nextDouble)
                if (nextDouble < prob) { mistype = MISType.InSet }
              }
            }
            (vdata._1, mistype)
          })
        println("finished putting randomly inset...")
        debugRDD(g.vertices.collect(), "vertices after setting inSet or not inSet")
        // Find whether any of the vertices have also picked itself into the matching
        println("resolving inset conflicts...")
        g = g.aggregateNeighborValues[VertexId](
          EdgeDirection.Out,
          (nbrId, nbrdata) => nbrdata._2 == MISType.InSet,
          (vid, vdata) => vdata._2 == MISType.InSet,
          (vid, vdata) => vid,
          Math.min,
          (vid, vdata, optAggrMsg) => {
            if (optAggrMsg.isDefined && optAggrMsg.get < vid) {
              println("setting type to Unknown. msg: " + optAggrMsg.get)
              (vdata._1, MISType.Unknown)
            } else { vdata }
          })
        g.vertices.persist; g.edges.persist
        debugRDD(g.vertices.collect(), "vertices after resolving MIS conflicts")
        // Find whether any of the neighbors are in set g =
        g = g.aggregateNeighborValues[Boolean](
          EdgeDirection.Out,
          (nbrId, nbrdata) => nbrdata._2 == MISType.InSet,
          (vid, vdata) => vdata._2 == MISType.Unknown,
          (vid, vdata) => true,
          (a, b) => true /* dummy aggregation of two true values */ ,
          (vid, vdata, optAggrMsg) => {
            if (optAggrMsg.isDefined) {
              assert(MISType.InSet != vdata._2, "if a nbr is in set, vertex cannot be inSet")
              (vdata._1, MISType.NotInSet)
            } else { vdata }
          })
        debugRDD(g.vertices.collect(), "vertices after setting NOTINSET...")
        // Remove edges from vertices in the MIS and not in MIS
        g = g.filterEdges(
          edgeTriplet => (MISType.Unknown == edgeTriplet.srcAttr._2) &&
            (MISType.Unknown == edgeTriplet.dstAttr._2))
        debugRDD(g.edges.collect(), "edges after removing edges from INSET and NOTINSET...")
        numUnknownVertices = g.aggregateGlobalValue[Int](
          v => if (v._2._2 == MISType.Unknown) 1 else 0, _ + _)
      }
      outputFoundVertices(g.vertices.filter(v => MISType.InSet == v._2._2), colorID)
      g = g.filterVertices((vid, vdata) => MISType.InSet != vdata._2)
      debugRDD(g.vertices.collect(), "new vertices after removing vertices from previous MIS...")

      val newG = oldGraph.outerJoinVertices(g.vertices)((vid, vd, opt) => (vd, opt.isDefined))
      g = newG.filterVertices((vid, vdata) => vdata._2).mapVertices((vid, vdata) => vdata._1)
      debugRDD(g.vertices.collect(), "vertices after forming new graph...")
      debugRDD(g.edges.collect(), "edges after forming new graph...")
      numUnknownVertices = g.numVertices
    }
  }

  private def correctEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VD, ED] = {
    val sc = graph.vertices.context
    val vset = sc.broadcast(graph.vertices.map(_._1).collect().toSet)
    val newEdges = graph.edges.filter(e => vset.value.contains(e.srcId)
      && vset.value.contains(e.dstId))
    Graph(graph.vertices, newEdges)
  }

  private def approximateMaxWeightMatching(graph: Graph[VertexId, Double]) = {
    var g = graph
    var iterNo = 0
    var numVertices = g.numVertices
    g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out, (vid, vvalues, edges) => !edges.isEmpty)
    while (numVertices > 0) {
      iterNo += 1
      println("starting iteration: " + iterNo)
      // Pick max weight edge
      g = g.updateVerticesUsingLocalEdges(
        EdgeDirection.Out,
        (vid, vdata, edges) => {
          var pickedNbrId = vid
          var pickedNbrWeight = Double.MinValue
          if (!edges.isEmpty) {
            for (edge <- edges) {
              if (edge.attr > pickedNbrWeight || (edge.attr == pickedNbrWeight
                && edge.dstId > pickedNbrId)) {
                pickedNbrWeight = edge.attr
                pickedNbrId = edge.dstId
              }
            }
          }
          pickedNbrId
        })
      // Check if match is successful
      g = g.updateSelfUsingAnotherVertex[VertexId](
        (vid, vdata) => true,
        (vid, vdata) => vdata /* where the id is stored, i.e., vdata itself */ ,
        (othervid, othervdata) => othervdata /* msg from other vertex */ ,
        (vid, vdata, othervval) => {
          if (othervval != vid) {
            println("vertexId: " + vid + " has failed to match")
            -1 /* failed match (two nbrs have NOT picked each other). new value is -1 */
          } else {
            println("vertexId: " + vid + " has succesfully matched")
            vdata /* successful match */ }
        })
      outputMatchedVertices(g.vertices.filter(v => v._2 >= 0), iterNo)
      // remove matched vertices
      g = g.filterVertices((vid, vdata) => vdata == -1)
      g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out, (vid, vdata, edges) => !edges.isEmpty)
      numVertices = g.numVertices
    }
  }

  private def boruvkasMinimumSpanningTree(graph: Graph[MSTVertexValue, MSTEdgeValue]) = {
    var g = graph
    var iterNo = 0
    var numVertices = g.numVertices
    g = g.filterVerticesUsingLocalEdges(EdgeDirection.Either,
      (vid, vdata, edges) => !edges.isEmpty)
    while (numVertices > 0) {
      iterNo += 1
      // Pick min weight edge
      g = g.updateVerticesUsingLocalEdges(
        EdgeDirection.Out,
        (vid, vdata, edges) => {
          var pickedNbrId = vid
          var pickedNbrOriginalSrcId, pickedNbrOriginalDstId = -1L
          var pickedNbrWeight = Double.MaxValue
          if (!edges.isEmpty) {
            for (edge <- edges) {
              if (edge.attr.weight < pickedNbrWeight || (edge.attr.weight == pickedNbrWeight
                && edge.dstId > pickedNbrId)) {
                pickedNbrWeight = edge.attr.weight
                pickedNbrId = edge.dstId
                pickedNbrOriginalSrcId = edge.attr.originalSrcId
                pickedNbrOriginalDstId = edge.attr.originalDstId
              }
            }
          }
          vdata.pickedNbrId = pickedNbrId
          vdata.pickedEdgeOriginalSrcId = pickedNbrOriginalSrcId
          vdata.pickedEdgeOriginalDstId = pickedNbrOriginalDstId
          vdata.pickedEdgeWeight = pickedNbrWeight
          vdata
        })
      // Find the roots of conjoined trees: let v.pickedNbrID = w.ID, if w.pickedNbrID is also 
      // equal to v.ID, then v and w are part of the cycle of the conjoined tree and the one with
      // the higher ID is the root.
      g = g.updateSelfUsingAnotherVertex[VertexId](
        (vid, vdata) => true,
        (vid, vdata) => vdata.pickedNbrId /* where the id of the other vertex is stored */ ,
        (othervid, othervdata) => othervdata.pickedNbrId,
        (vid, vdata, othervval) => {
          if (othervval == vid && vid > vdata.pickedNbrId) {
            vdata.pickedNbrId = vid
          }
          vdata
        })
      // Find the root of the conjoined tree: updating self.pickedNbrID to
      // self.pickedNbrID.pickedNbrID until pickedNbrIDs converge.
      g = pointerJumping(g, (vid, vdata) => vdata.pickedNbrId,
        (vid, vdata, parentsParent) => { vdata.pickedNbrId = parentsParent; vdata })
      outputVertices(g.vertices.filter(v => v._1 != v._2.pickedNbrId), iterNo)
      // form supervertices
      g = g.formSupervertices((vid, vdata) => vdata.pickedNbrId,
        vertexValues => new MSTVertexValue(-1, -1, -1, -1.0),
        edges => {
          var minWeightEdge = edges(0)
          for (index <- 1 to (edges.length - 1)) {
            if (edges(index).weight < minWeightEdge.weight
              || (edges(index).weight == minWeightEdge.weight &&
                edges(index).originalDstId > minWeightEdge.originalDstId)) {
              minWeightEdge = edges(index)
            }
          }
          minWeightEdge
        })

      // Remove singletons
      g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out, (vid, vdata, edges) => !edges.isEmpty)
      numVertices = g.numVertices
    }
  }

  /**
   * This is the pointer jumping algorithm used in Boruvka's MST algorithm and also in METIS
   * algorithm's coarsening stage. Initially each vertex has a field called the pointerField that
   * contains the id of another vertex (call it its parent), which can possibly be itself
   * (actually some vertices should  point to themselves for convergence). Then vertices in
   * iterations keep updating their parents to point to their latest "parent's" parent until
   * the pointers converge. In that sense, this is a transitive closure-like algorithm. Both in
   * MST and METIS after this operation all vertices that point to the same vertex by forming
   * super-vertex formation.
   */
  def pointerJumping[VD: Manifest, ED: Manifest](g: Graph[VD, ED],
    pointerFieldF: (VertexId, VD) => VertexId,
    setF: (VertexId, VD, VertexId) => VD): Graph[VD, ED] = {
    var numDiff = Long.MaxValue
    var oldG = g
    while (numDiff > 0) {
      var newG = oldG.updateSelfUsingAnotherVertex[VertexId](
        (vid, vdata) => true,
        pointerFieldF,
        pointerFieldF,
        (vid, vdata, othervval) => setF(vid, vdata, othervval))
      val keyValueOldVertices = oldG.vertices.map { v => (v._1, v._2) }
      val keyValueNewVertices = newG.vertices.map { v => (v._1, v._2) }
      val oldNewVerticesJoined = keyValueOldVertices.join(keyValueNewVertices)
      val numDiffRDD = oldNewVerticesJoined.flatMap { idOldNewVertex =>
        val oldID = pointerFieldF(idOldNewVertex._1, idOldNewVertex._2._1)
        val newID = pointerFieldF(idOldNewVertex._1, idOldNewVertex._2._2)
        if (!oldID.equals(newID)) { Some(1) }
        else { None }
      }
      numDiff = numDiffRDD.count()
      oldG = newG
    }
    oldG
  }

  // The first column of the vertex value is the colorID, the second is an indicator for
  // whether the vertex has found its component
  private def stronglyConnectedComponents(graph: Graph[(VertexId, Boolean), Int]) = {
    var g = graph
    var iterNo = 0
    while (g.numVertices > 0) {
      iterNo += 1
      g = g.updateVertices((vid, vdata) => true, (vid, vdata) => (vid, false))
      g = g.propagateAndAggregate[VertexId](
        EdgeDirection.Out,
        (vid, vdata) => true,
        (vid, vdata) => vdata._1,
        (msg, edata, nbrdata) => msg,
        Math.max,
        (vid, vdata, aggrMsg) => (Math.max(vdata._1, aggrMsg), false))
      g = g.updateVertices((vid, vdata) => true, (vid, vdata) =>
        { if (vdata._1 == vid) (vdata._1, true) else vdata })
      g = g.propagateAndAggregate[VertexId](
        EdgeDirection.In /* in transpose */ ,
        // start from vertices that have their colorID == ID
        (vid, vdata) => vdata._2,
        (vid, vdata) => vdata._1,
        (msg, edata, nbrdata) => { if (nbrdata._1 == msg) msg else -1L },
        Math.max,
        (vid, vdata, aggrMsg) => {
          if (aggrMsg > 0 && !vdata._2) (vdata._1, true) else vdata
        })
      outputFoundVertices(g.vertices.filter(v => v._2._2))
      g = g.filterVertices((vid, vdata) => !vdata._2)
    }
  }

  // The hubs and authority values are stored in the first and second columns of the vertex value,
  // respectively
  private def hits(graph: Graph[(Double, Double), Int]) = {
    var g = graph
    for (i <- 1 to 10) {
      g = g.aggregateNeighborValues[Double](
        EdgeDirection.In,
        (nbrid, nbrdata) => true,
        (vid, vdata) => true,
        (vid, vdata) => vdata._1,
        (_ + _),
        (vid, vdata, optAggrMsg) => {
          if (optAggrMsg.isEmpty) vdata
          else (vdata._1, optAggrMsg.get)
        })
      val authNorm = math.sqrt(g.aggregateGlobalValue[Double](v => v._2._2 * v._2._2, _ + _))
      g = g.updateVertices((vid, vdata) => true, (vid, vdata) => (vdata._1, vdata._2 / authNorm))
      g = g.aggregateNeighborValues[Double](
        EdgeDirection.Out,
        (nbrid, nbrdata) => true,
        (vid, vdata) => true,
        (vid, vdata) => vdata._2,
        (_ + _),
        (vid, vdata, optAggrMsg) => {
          if (optAggrMsg.isEmpty) vdata
          else (optAggrMsg.get, vdata._2)
        })
      val hubsNorm = math.sqrt(g.aggregateGlobalValue[Double](v => v._2._1 * v._2._1, _ + _))
      g = g.updateVertices((vid, vdata) => true, (vid, vdata) => (vdata._1 / hubsNorm, vdata._2))
    }
    // POSTPROCESSING
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex._1 + " hubs: " + vertex._2._1 + " authority: " + vertex._2._2)
    }
  }

  private def pageRank(graph: Graph[Double, Int]) = {
    var g = graph
    val numVertices = g.numVertices
    val initialPageRankValue = 1.0 / numVertices
    println("initialPageRankValue: " + initialPageRankValue)
    g = graph.updateVertices((vid, vdata) => true, (vid, vdata) => initialPageRankValue)
    for (i <- 1 to 10) {
      g = g.aggregateNeighborValues[Double](EdgeDirection.In,
        (nbrId, nbrdata) => true, // aggregate all neighbors
        (vid, vdata) => true, // aggregate all vertices
        (vid, vdata) => vdata,
        (_ + _),
        (vid, vdata, optAggrMsg) => {
          if (optAggrMsg.isEmpty) vdata
          else 0.15 / numVertices + 0.85 * optAggrMsg.get
        })
    }
    // POSTPROCESSING
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex._1 + " pageRank: " + vertex._2)
    }
  }

  private def weaklyConnectedComponents(graph: Graph[VertexId, Int]): Graph[VertexId, Int] = {
    var g = graph
    g = graph.updateVertices((vid, vdata) => true, (vid, vdata) => vid)
    g = g.propagateAndAggregate[VertexId](EdgeDirection.Out,
      (vid, vdata) => true /* start from all vertices */ ,
      (vid, vdata) => vdata,
      (msg, edata, nbrdata) => msg,
      Math.max,
      (vid, vdata, aggrMsg) => Math.max(vdata, aggrMsg))
    // POSTPROCESSING
    var localComponentIDAndSizes = g.vertices.map { v => (v._2, 1) }.reduceByKey(_ + _).collect()
    for (componentIDSize <- localComponentIDAndSizes) {
      println("componentID: " + componentIDSize._1 + " size: " + componentIDSize._2)
    }
    g
  }

  private def singleSourceShortestPaths(graph: Graph[Int, Int], srcId: VertexId) = {
    var g = graph
    g = graph.updateVertices((vid, vdata) => vid == srcId, (vid, vdata) => 0)
    g = g.propagateAndAggregate[Int](
      EdgeDirection.Out,
      (vid, vdata) => vid == srcId /* start vertex */ ,
      (vid, vdata) => vdata /* start vertex */,
      (msg, evals, nbrdata) => msg + evals,
      Math.min,
      (vid, vdata, aggrMsg) => Math.min(vdata, aggrMsg))
    // POSTPROCESSING
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex._1 + " distance to vertex 5: " + vertex._2)
    }
  }

  private def outputFoundVertices(verticesToOutput: VertexRDD[(VertexId, MISType)], colorID: Int) {
    println("starting outputting colored vertices...")
    val localColoredVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile("/Users/semihsalihoglu/projects/graphx/spark/output/" +
      "gc-results/gc_test_output_" + colorID)
    for (vertex <- localColoredVertices) {
      println("vertexId: " + vertex._1 + " colorID:" + colorID)
    }
    println("finished outputting colored vertices...")
  }

  def debugRDD[A](arrayToDebug: Array[A], debugString: String) {
    println("starting to debug " + debugString)
    println(arrayToDebug.deep.mkString("\n"))
    println("finished debugging " + debugString)
  }

  private def outputMatchedVertices[A](verticesToOutput: VertexRDD[A], iterNo: Int) {
    println("starting outputting matched vertices...")
    val localFoundVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile("/Users/semihsalihoglu/projects/graphx/spark/output/" +
      "mwm-results/mst_test_output" + iterNo)
    for (vertex <- localFoundVertices) {
      println("vertexId: " + vertex._1 + " matchedVertex:" + vertex._2)
    }
    println("finished outputting matched vertices...")
  }

  private def outputFoundVertices(foundVertices: VertexRDD[(VertexId, Boolean)]) {
    println("starting outputting found vertices...")
    val localFoundVertices = foundVertices.collect
    for (vertex <- localFoundVertices) {
      println("vertexId: " + vertex._1 + " sccID: " + vertex._2._1)
    }
    println("finished outputting found vertices...")
  }

  private def outputVertices(verticesToOutput: VertexRDD[MSTVertexValue], iterNo: Int) = {
    println("starting outputting pickedEdges ...")
    val localFoundVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile(
      "/Users/semihsalihoglu/projects/graphx/spark/output/mst-results/mst_test_output" + iterNo)
    for (vertex <- localFoundVertices) {
      println("vertexId: " + vertex._1 + " pickedEdge: (" + vertex._2.pickedEdgeOriginalSrcId
        + ", " + vertex._2.pickedEdgeOriginalDstId + ")")
    }
    println("finished outputting colorIDs...")
  }

  class BCVertexValue(var bfsLevel: Int, var sigma: Double, var delta: Double, var bc: Double)
    extends Serializable {
    override def toString = {
      "bfsLevel: " + bfsLevel + " sigma: " + sigma + " delta: " + delta +
        " bc: " + bc
    }
    override def clone(): BCVertexValue = {
      new BCVertexValue(bfsLevel, sigma, delta, bc)
    }
  }

  sealed abstract class MISType {
    def reverse: MISType = this match {
      case MISType.InSet => MISType.InSet
      case MISType.NotInSet => MISType.NotInSet
      case MISType.Unknown => MISType.Unknown
    }
  }

  object MISType {
    case object InSet extends MISType
    case object NotInSet extends MISType
    case object Unknown extends MISType
  }

  class MSTVertexValue(var pickedNbrId: VertexId, var pickedEdgeOriginalSrcId: VertexId,
    var pickedEdgeOriginalDstId: VertexId, var pickedEdgeWeight: Double) extends Serializable {
    override def toString = {
      "pickedNbrId: " + pickedNbrId + " pickedEdgeOriginalSrcId: " +
        pickedEdgeOriginalSrcId + " pickedEdgeOriginalDstId: " + pickedEdgeOriginalDstId +
        " pickedEdgeWeight: " + pickedEdgeWeight
    }
  }

  class MSTEdgeValue(var weight: Double, var originalSrcId: VertexId, var originalDstId: VertexId)
    extends Serializable {
    override def toString = {
      "weight: " + weight + " originalSrcId: " + originalSrcId + " originalDstId: " + originalDstId
    }
  }

  class MetisVertexValue(var weight: Double, var superVertexID: VertexId, var partitionID: Int,
    var tentativePartitionID: Int) extends Serializable {
    override def toString = {
      "weight: " + weight + " superVertexID: " + superVertexID +
        " partitionID: " + partitionID + " tentativePartitionID: " + tentativePartitionID
    }
  }
}
