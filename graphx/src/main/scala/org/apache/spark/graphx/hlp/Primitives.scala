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
package org.apache.spark.graphx.hlp

import scala.reflect.ClassTag
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphOps
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.graphx._

/**
 * High-level primitives for programming algorithms. This class is implicitly constructed for each
 * Graph object.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
class Primitives[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

 /**
   * Filters the edges of the graph by keeping only the edges that satisfy the
   * given predicate. The predicate takes as input the src and destination vertex ids and values.
   *
   * @param epred the edge predicate
   */
  def filterEdges(epred: EdgeTriplet[VD, ED] => Boolean): Graph[VD, ED] = {
    graph.subgraph(epred, (vid, vval) => true)
  }

  /**
   * Filters the vertices of the graph by keeping only the vertices that satisfy the
   * given predicate. The predicate takes as input the vertex id and vertex value.
   *
   * @param vpred the vertex predicate
   */
  def filterVertices(vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = {
    graph.subgraph(e => true, vpred)
  }

  /**
   * Filters the vertices of the graph by keeping only the vertices that satisfy the
   * given predicate. The predicate takes as input the vertex id, vertex value and a
   * list of edges in the user-specified direction.
   *
   * @param edgeDirection the direction along which to collect the local edges
   * @param vpred the vertex predicate
   * 
   * @return the resulting graph at the end of the computation
   */
  def filterVerticesUsingLocalEdges(edgeDirection: EdgeDirection, 
    vpred: (VertexId, VD, Array[Edge[ED]]) => Boolean): Graph[VD, ED] = {
    // Sets the values of vertices that will be filtered to None, others to Some
    val graphWithOptVals = graph.outerJoinVertices(graph.ops.collectEdges(edgeDirection)) {
      (vid, vdata, localEdgesOpt) => vpred(vid, vdata, localEdgesOpt.getOrElse(
        Array.empty[Edge[ED]]))
    }
    graph.mask(graphWithOptVals.subgraph(edgeTriplet => true, (vid, boolVal) => boolVal))
  }

  /**
   * Transforms each vertex attribute in the graph using the updateF function. It takes as input
   * a predicate which filters the vertices that should be updated and an updateF function to
   * update those vertices.
   * 
   * @note The new graph has the same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param vP predicate to select which vertices to udpate
   * @param updateF the function from a vertexID, vertex value, and a list of edges to a new vertex
   *        value
   * 
   * @return the resulting graph at the end of the computation
   */
  def updateVertices(vP: (VertexId, VD) => Boolean,
    updateF: (VertexId, VD) => VD): Graph[VD, ED] = {
    graph.mapVertices((vid, vdata) => if (vP(vid, vdata)) updateF(vid, vdata) else vdata)    
  }
  
  /**
   * Transforms each vertex attribute in the graph using the map function, which
   * takes as input the vertexID, vertex value, and a list of edges of the vertex in
   * the user-specified direction 
   *
   * @Warning: This functionality of this function can be achieved more efficiently by using
   * mapReduceTriplets, if the values on the edges of vertices can be aggregated with an associative
   * and commutative function. For example, if we want to udpate the value of each vertex v by
   * setting the maxInEdge field of v's value to the maximum in-coming edge it has, this computation
   * can be expressed in two ways:
   * 1) g.mapNeighborhoods(EdgeDirection.In, (vid, vvalue, inedges) => 
   *                                          vvalue.maxInEdge = max(inedges)}
   * 2) val messages = g.mapReduceTriplets(triplet => 
   *                                       (triplet.srcId, triplet.attr), (a, b) => Math.max(a, b))
   *    g.vertices.outherJoin(messages)((vid, vvalue, maxEdge) => vvalue.maxInEdge = maxEdge)
   * Prefer the 2nd method for efficiency. For non-commutative operations or non-associative
   * operations or for operations that are difficult to write in a commutative/associative way,
   * prefer the 1st method.
   * 
   * @note The new graph has the same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param edgeDirection the direction along which to collect the local edges
   * @param map the function from a vertexID, vertex value, and a list of edges to a new vertex
   *        value
   *
   * @tparam VD2 the new vertex data types
   * 
   * @return the resulting graph at the end of the computation
   */
  def updateVerticesUsingLocalEdges[VD2: ClassTag](edgeDirection: EdgeDirection, 
    f: (VertexId, VD, Array[Edge[ED]]) => VD2): Graph[VD2, ED] = {
    val localEdges = graph.ops.collectEdges(edgeDirection)
    graph.outerJoinVertices(localEdges) { (vid, vdata, localEdgesOpt) => 
      f(vid, vdata, localEdgesOpt.getOrElse(Array.empty[Edge[ED]])) }
  }
  
  /**
   * Updates the value of each vertex v, for which vPred evaluates to true, by aggregating the
   * values of v's neighbors in the specified direction. Only the vertices which evaluate to true
   * on the given vertex predicate are updated. Other vertices remain unchanged. And only the
   * values of those neighbors which evaluate to true on the given neighbor predicate are
   * aggregated. Used in algorithms like one iteration of pagerank, hits, conductance, and others.
   * 
   * @param edgeDirection the direction along which to collect the local edges
   * @param nbrPred selects which neighbors' values to aggregate
   * @param vPred selects which vertices to update
   * @param aggregatedValueF extracts the relevant part of v's neighbor's value that
   *        is needed to update v
   * @param aggregateF aggregates two neighbor's values
   * @param updateF given a vertex v's id, value and the aggregated values of v's neighbors
   *        returns a new value for v.
   * 
   * @return the resulting graph at the end of the computation
   */
  def aggregateNeighborValues[U: ClassTag](edgeDirection: EdgeDirection,
    nbrPred: (VertexId, VD) => Boolean, vPred: (VertexId, VD) => Boolean, 
    aggregatedValueF: (VertexId, VD) => U, aggregateF: (U, U) => U, 
    updateF: (VertexId, VD, Option[U]) => VD): Graph[VD, ED] = {    
    val edgeDirectionToSendMsgs = edgeDirection match {
      case EdgeDirection.Either => EdgeDirection.Either
      case EdgeDirection.In => EdgeDirection.Out
      case EdgeDirection.Out => EdgeDirection.In
    }
    val messages = graph.mapReduceTriplets(
      sendMessageFForAggregateNeighborValues[U](edgeDirectionToSendMsgs, vPred, nbrPred,
        aggregatedValueF, (aggrValue, edgeValue) => aggrValue), aggregateF, None)
    graph.outerJoinVertices(messages)((vid, vdata, optAggrValue) =>
      if (vPred(vid, vdata)) updateF(vid, vdata, optAggrValue) else vdata)
  }

  private def sendMessageFForAggregateNeighborValues[U: ClassTag](dirToSendMessage: EdgeDirection,
    vPred: (VertexId, VD) => Boolean, nbrPred: (VertexId, VD) => Boolean,
    sendValueFromVertexF: (VertexId, VD) => U, sendAlongEdgeF: (U, ED) => U): 
    ActiveEdgeTriplet[VD, ED] => Iterator[(VertexId, U)] = {
    (edge: ActiveEdgeTriplet[VD, ED]) => {
        val msgToSrcVertex = (edge.srcId,
          sendAlongEdgeF(sendValueFromVertexF(edge.dstId, edge.dstAttr), edge.attr))
        val msgToDstVertex = (edge.dstId,
          sendAlongEdgeF(sendValueFromVertexF(edge.srcId, edge.srcAttr), edge.attr))
        // Below a message is sent only if the nbr sending the message evaluates to true for
        // nbrPred and the receiving vertex evaluates to true for the vPred.
        dirToSendMessage match {
          case EdgeDirection.Either => {
            if (vPred(edge.srcId, edge.srcAttr) && nbrPred(edge.dstId, edge.dstAttr)
              && vPred(edge.dstId, edge.dstAttr) && nbrPred(edge.srcId, edge.srcAttr)) {
              Iterator(msgToSrcVertex, msgToDstVertex)
            }
            // If the src vertex evaluates to true for vPred, and the dst vertex evaluates to 
            // true for the nbrPred, then the nbr, i.e. dst, can send its value to vertex, i.e. src
            else if (vPred(edge.srcId, edge.srcAttr) && nbrPred(edge.dstId, edge.dstAttr)) {
              Iterator(msgToSrcVertex)
            }
            // Exactly the opposite scenario of the above comment
            else if (vPred(edge.dstId, edge.dstAttr) && nbrPred(edge.srcId, edge.srcAttr)) {
              Iterator(msgToDstVertex)
            }
            else Iterator.empty
          }
          case EdgeDirection.Out =>
            if (nbrPred(edge.srcId, edge.srcAttr) && vPred(edge.dstId, edge.dstAttr)) {
              Iterator(msgToDstVertex)
            } else Iterator.empty
          case EdgeDirection.In =>
            if (nbrPred(edge.dstId, edge.dstAttr) && vPred(edge.srcId, edge.srcAttr)) {
              Iterator(msgToSrcVertex) 
            } else Iterator.empty
          case EdgeDirection.Both =>
            throw new SparkException("aggregateNeighborValues does not support" +
              "EdgeDirection.Both. Use EdgeDirection.Either instead.")
        }
      }
  }

  /**
   * Iterative version of aggregateNeighborValues. In the first iteration, one or more vertices
   * start propagating a value to their neighbors in a user-specified direction. Vertices that
   * receive propagated values aggregate them and update their own values. In the next iteration,
   * all vertices whose values have changed propagate a new value to their neighbors. The
   * propagation of values continues in iterations until all vertex values converge.
   * 
   * This primitive is similar to the Pregel operation below, but there are differences in its
   * behavior. First, there is no initial message. Second, we can select which vertices to start
   * propagating from. Third, the propagation continue only from vertices whose values have
   * changed, (in Pregel, it continues from vertices that received at least one message). And
   * finally there is no maximum number of iterations.
   * 
   * This is the core operation for algorithms like, weakly connected components, single source
   * shortest paths and also appears in strongly connected components, conductance,
   * betweenness-centrality, and k-core.
   * 
   * @param edgeDirection the direction along which to propagate values to
   * @param startVPred selects which vertices to start the propagation from
   * @param propagatedValueF extracts the relevant part of a vertex v's value to propagate v's
   *        neighbors
   * @param propagateAlongEdgeF given the output of propagatedValueF and an edge to propagate the
   *        value from, possibly returns a modified value to propagate along the edge
   * @param aggregateF aggregates two values that are being propagated to the same vertex
   * @param updateF given a vertex v's id, value and the aggregation of the propagated values to v,
   *        returns a new value for v.
   * 
   * @return the resulting graph at the end of the computation
   */
  def propagateAndAggregate[U: ClassTag](edgeDirection: EdgeDirection,
    startVPred: (VertexId, VD) => Boolean, propagatedValueF: (VertexId, VD) => U,
    propagateAlongEdgeF: (U, ED) => U, aggregateF: (U, U) => U,
    updateF: (VertexId, VD, U) => VD): Graph[VD, ED] = {
    var g = graph
    var messages = g.mapReduceTriplets(
      sendMessageFForPropagateAndAggregate(edgeDirection,
        /* send a message from the source of the edge if it satisfies startVPred */
        activeEdgeTriplet => startVPred(activeEdgeTriplet.srcId, activeEdgeTriplet.srcAttr),
        /* send a message from the destination of the edge if it satisfies startVPred */
        activeEdgeTriplet => startVPred(activeEdgeTriplet.dstId, activeEdgeTriplet.dstAttr),
        propagatedValueF, propagateAlongEdgeF),
      aggregateF)
    var activeMessages = messages.count()
    // Loop until no more messages are being sent
    var prevG: Graph[VD, ED] = null
    while (activeMessages > 0) {
      // Receive the propagated values/messages. The diff operation ensures that vertices whose
      // values have not changed do not appear in changedVerts.
      val changedVerts = g.vertices.diff(g.vertices.innerJoin(messages)(updateF)).cache()
      // Update the graph with the changed vertices.
      prevG = g
      g = g.outerJoinVertices(changedVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.cache()

      val oldMessages = messages
      messages = g.mapReduceTriplets(
        sendMessageFForPropagateAndAggregate(edgeDirection,
          // send a message from the source of the edge if it is active (i.e. its value changed)
          activeEdgeTriplet => activeEdgeTriplet.srcActive,
          // send a message from the destination of the edge if it is active
          // (i.e. its value changed)
          activeEdgeTriplet => activeEdgeTriplet.dstActive,
          propagatedValueF, propagateAlongEdgeF),
        aggregateF, Some((changedVerts, edgeDirection))).cache()
      activeMessages = messages.count()
      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      changedVerts.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
    }
    g
  }

  private def sendMessageFForPropagateAndAggregate[U: ClassTag](
    dirToSendMessage: EdgeDirection,
    sendMessageFromSrcPred: ActiveEdgeTriplet[VD, ED] => Boolean,
    sendMessageFromDstPred: ActiveEdgeTriplet[VD, ED] => Boolean,
    propagateValueF: (VertexId, VD) => U,
    propagateAlongEdgeF: (U, ED) => U): ActiveEdgeTriplet[VD, ED] => Iterator[(VertexId, U)] = {
    (edge: ActiveEdgeTriplet[VD, ED]) =>
      {
        val msgToSrcVertex = (edge.srcId,
          propagateAlongEdgeF(propagateValueF(edge.dstId, edge.dstAttr), edge.attr))
        val msgToDstVertex = (edge.dstId,
          propagateAlongEdgeF(propagateValueF(edge.srcId, edge.srcAttr), edge.attr))
        // Below a message is sent from the src (dst) vertex v of an edge if sendMessageFromSrcPred
        // (sendMessageFromDstPred)evaluates to true on v.
        dirToSendMessage match {
          case EdgeDirection.Either => {
            if (sendMessageFromSrcPred(edge) && sendMessageFromDstPred(edge)) {
              Iterator(msgToSrcVertex, msgToDstVertex)
            }
            else if (sendMessageFromSrcPred(edge)) { Iterator(msgToDstVertex) }
            else if (sendMessageFromDstPred(edge)) { Iterator(msgToSrcVertex) }
            else Iterator.empty
          }
          case EdgeDirection.Out =>
            if (sendMessageFromSrcPred(edge)) { Iterator(msgToDstVertex) }
            else Iterator.empty
          case EdgeDirection.In =>
            if (sendMessageFromDstPred(edge)) { Iterator(msgToSrcVertex) }
            else Iterator.empty
          case EdgeDirection.Both =>
            throw new SparkException("propagateAndAggregate does not support" +
              "EdgeDirection.Both. Use EdgeDirection.Either instead.")
        }
      }
  }

  /**
   * Vertices store a pointer (actually an ID of) another vertex, not necessarily a neighbor, in
   * their values. If v points at w, and vPred(v) returns true, then v updates the value of w using
   * v's value. This operation appears often in matching algorithms.
   * 
   * @param vPred selects which vertices will update the vertices they point to
   * @param idF given a vertex v's id and its value extracts the id of the vertex w that v points to
   * @param relevantValueF extracts the relevant part from v's value to update the vertex w that v 
   *        points to
   * @param aggregateRelevantValuesF if more than one vertices are trying to update a vertex w,
   *        then aggregates the relevant values from those vertices
   *        Warning: aggregateRelevantValuesF is called only if there are two or more values mapped.
   * @param updateF given a vertex v's id, value and the aggregated value from the vertices that
   *        want to update v, returns a new value for v
   * 
   * @return the resulting graph at the end of the computation
   */
  def updateAnotherVertexUsingSelf[U: ClassTag](vPred: (VertexId, VD) => Boolean,
    idF: (VertexId, VD) => VertexId, relevantValueF: (VertexId, VD) => U,
    aggregateRelevantValuesF: List[U] => U,
    updateF: (VertexId, VD, U) => VD): Graph[VD, ED] = {
    val messages = graph.vertices.flatMap(vidVvals => {
      if (vPred(vidVvals._1, vidVvals._2)) {
        Iterator((idF(vidVvals._1, vidVvals._2), List(relevantValueF(vidVvals._1, vidVvals._2))))
      }
      else Iterator.empty
    }).reduceByKey(_ ::: _)
      .map(vidListOfValues => (vidListOfValues._1,
        if (vidListOfValues._2.size > 1) aggregateRelevantValuesF(vidListOfValues._2)
        else vidListOfValues._2(0)))
    graph.outerJoinVertices(messages) { (vid, old, newOpt) =>
      if (newOpt.isDefined) updateF(vid, old, newOpt.get) else old }
  }
  
  /**
   * Similar to updateAnotherVertexUsingSelf, except now vertices update themselves using a value
   * from the vertices that they point to. Since now each vertex is updated by exactly one vertex,
   * we do not need an aggregation function. Again used in some matching algorithms, but also in
   * pointer jumping operations found in Boruvka's minimum spanning tree, METIS, and some
   * multi-level clustering algorithms. 
   * 
   * @param vPred selects which vertices will update themselves using the vertices they point to
   * @param idF given a vertex v's id and its value extracts the id of the vertex w that v points to
   * @param relevantValueF extracts the relevant part from the vertex w that v points to, to update
   *        v's value.
   * @param updateF given a vertex v's id, value and the value extracted from the vertex w that v
   *        points to, returns a new value for v
   * 
   * @return the resulting graph at the end of the computation
   */
  def updateSelfUsingAnotherVertex[U: ClassTag](vPred: (VertexId, VD) => Boolean,
    idF: (VertexId, VD) => VertexId, relevantValueF: (VertexId, VD) => U, 
    updateF: (VertexId, VD, U) => VD): Graph[VD, ED] = {
    val verticesPointingToEachVertex = graph.vertices.flatMap(vidVvals => {
      if (vPred(vidVvals._1, vidVvals._2)) {
        Iterator((idF(vidVvals._1, vidVvals._2), List(vidVvals._1)))
      } else Iterator.empty
    }).reduceByKey(_ ::: _)
    val messages = VertexRDD(graph.vertices.join(verticesPointingToEachVertex).flatMap(
      vidVvalsAndPointingVertices => {
        val vid = vidVvalsAndPointingVertices._1
        val vvals = vidVvalsAndPointingVertices._2._1
        val pointingVertices = vidVvalsAndPointingVertices._2._2
        if (pointingVertices.isEmpty) Iterator.empty
        else {
          var msgsToSend: List[(VertexId, U)] = List()
          val msgToSend = relevantValueF(vid, vvals)
          for (pointingVertex <- pointingVertices) { msgsToSend ::= (pointingVertex, msgToSend)}
          msgsToSend
        }
      }))
    graph.outerJoinVertices(messages) { (vid, old, newOpt) =>
      if (newOpt.isDefined) updateF(vid, old, newOpt.get) else old }
  }

  /**
   * Aggregates a single global value over the vertices of the graph. A few examples use cases are
   * to compute the norm of the graph, to find the vertex with maximum distance from a given one
   * and many others.
   * 
   * @param mapF each vertex emits a value
   * @param reduceF aggregates the emitted values from the vertices (has to be commutative and
   *        associative)
   * 
   * @return the aggregated single value from the map and reduce functions
   */
  def aggregateGlobalValue[U: ClassTag](mapF: ((VertexId, VD)) => U, reduceF: (U, U) => U): U = {
    graph.vertices.map[U](mapF).reduce(reduceF)
  }

  /**
   * Similar to aggregateGlobalValue, except the map function also takes as input the edges
   * incident to each vertex in the specified direction.
   * 
   * @param edgeDirection the direction along which to collect the local edges
   * @param mapF each vertex emits a value
   * @param reduceF aggregates the emitted values from the vertices (has to be commutative and
   *        associative)
   * 
   * @return the aggregated single value from the map and reduce functions
   */
  def aggregateGlobalValueWithLocalEdges[U: ClassTag](
      edgeDirection: EdgeDirection, mapF: ((VertexId, (VD, Array[Edge[ED]]))) => U,
      reduceF: (U, U) => U): U = {
    val localEdges = graph.ops.collectEdges(edgeDirection)
    graph.vertices.join(localEdges).map[U](mapF).reduce(reduceF)
  }

  /**
   * Picks a random vertex from the graph and returns its ID.
   */
  def pickRandomVertex(): VertexId = {
    val probability = 50 / graph.numVertices
    var found = false
    var retVal: VertexId = null.asInstanceOf[VertexId]
    while (!found) {
      val selectedVertices = graph.vertices.flatMap { vidVvals =>
        if (Random.nextDouble() < probability) { Some(vidVvals._1) }
        else { None }
      }
      if (selectedVertices.count > 1) {
        found = true
        val collectedVertices = selectedVertices.collect()
        retVal = collectedVertices(Random.nextInt(collectedVertices.size))
      }
    }
   retVal
  }

  /**
   * Given a graph, forms a new graph by merging vertices into supervertices. Appears in
   * multi-level graph algorithms as well as in Boruvka's minimum spanning tree. Here's the typical
   * scenario when this primitive is used:
   * (1) After some computation in the algorithm, every vertex identifies the supervertex (possibly
   * itself) that it will merge into. This is extracted from the vertex by calling the
   * supervertexIdFieldF argument.
   * (2) All vertices and their values that belong to the same supervertex are merged into a single
   *     vertex. How the vertex values are merged is algorithm-specific is specified in the
   *     vertexAggrF argument.
   * (3) Consider an edge (u, v) and assume that vertices u and v are merged into supervertices s1
   *     and s2, respectively. If s1 = s2, then (u, v) is removed from the graph. Otherwise, (u, v)
   *     becomes an edge between s1 and s2. If there are multiple edges between s1 and s2, then
   *     edges are merged. How the edge values are merged is algorithm-specific and specified in
   *     the edgeAggrF argument.
   *
   * Note: If the users do not want to remove self loops, i.e., the case when s1 == s2, then we can
   * simply add a new boolean argument removeSelfLoops to this function.
   *
   * @param supervertexIdFieldF given a vertex v's id and attribute, extracts the supervertexId of v
   * @param vertexAggrF given the list of values of all the vertices that will merge into the same
   *        supervertex, returns the single merged value of the supervertex
   * @param edgeAggrF given a list of edges between the same two supervertices, merges them to form
   *         a single supervertex
   *
   * @return the resulting graph consisting of supervertices and the merged edges between them
   */
  def formSupervertices(supervertexIdFieldF: (VertexId, VD) => VertexId, vertexAggrF: Seq[VD] => VD,
    edgeAggrF: Seq[ED] => ED): Graph[VD, ED] = {
    val vertexRDD = graph.vertices.map(vidVvals => (supervertexIdFieldF(vidVvals._1, vidVvals._2),
      vidVvals._2)).groupByKey().map(v => (v._1, vertexAggrF(v._2)))
    val edgeRDD = graph.triplets.flatMap(triplet => {
      val newSrcId = supervertexIdFieldF(triplet.srcId, triplet.srcAttr)
      val newDstId = supervertexIdFieldF(triplet.dstId, triplet.dstAttr)
      if (newSrcId == newDstId) {
        Iterator.empty
      } else {
        Iterator(((newSrcId, newDstId), triplet.attr))
      }
    }).groupByKey().map(e => new Edge(e._1._1, e._1._2, edgeAggrF(e._2)))
    Graph(vertexRDD, edgeRDD)
  }
} // end of Primitives
