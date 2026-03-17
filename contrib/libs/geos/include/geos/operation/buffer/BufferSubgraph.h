/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/BufferSubgraph.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_BUFFERSUBGRAPH_H
#define GEOS_OP_BUFFER_BUFFERSUBGRAPH_H

#include <geos/export.h>

#include <geos/operation/buffer/RightmostEdgeFinder.h> // for composition

#include <vector>
#include <set>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Envelope;
}
namespace algorithm {
class CGAlgorithms;
}
namespace geomgraph {
class DirectedEdge;
class Node;
}
}

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/**
 * \brief
 * A connected subset of the graph of DirectedEdge and geomgraph::Node.
 *
 * Its edges will generate either
 * - a single polygon in the complete buffer, with zero or more holes, or
 * -  ne or more connected holes
 */
class GEOS_DLL BufferSubgraph {
private:
    RightmostEdgeFinder finder;

    std::vector<geomgraph::DirectedEdge*> dirEdgeList;

    std::vector<geomgraph::Node*> nodes;

    geom::Coordinate* rightMostCoord;

    geom::Envelope* env;

    /** \brief
     * Adds all nodes and edges reachable from this node to the subgraph.
     *
     * Uses an explicit stack to avoid a large depth of recursion.
     *
     * @param startNode a node known to be in the subgraph
     */
    void addReachable(geomgraph::Node* startNode);

    /// Adds the argument node and all its out edges to the subgraph
    ///
    /// @param node the node to add
    /// @param nodeStack the current set of nodes being traversed
    ///
    void add(geomgraph::Node* node, std::vector<geomgraph::Node*>* nodeStack);

    void clearVisitedEdges();

    /** \brief
     * Compute depths for all dirEdges via breadth-first traversal
     * of nodes in graph
     *
     * @param startEdge edge to start processing with
     */
    // <FIX> MD - use iteration & queue rather than recursion, for speed and robustness
    void computeDepths(geomgraph::DirectedEdge* startEdge);

    void computeNodeDepth(geomgraph::Node* n);

    void copySymDepths(geomgraph::DirectedEdge* de);

    bool contains(std::set<geomgraph::Node*>& nodes, geomgraph::Node* node);

public:

    friend std::ostream& operator<< (std::ostream& os, const BufferSubgraph& bs);

    BufferSubgraph();

    ~BufferSubgraph();

    std::vector<geomgraph::DirectedEdge*>* getDirectedEdges();

    std::vector<geomgraph::Node*>* getNodes();

    /** \brief
     * Gets the rightmost coordinate in the edges of the subgraph
     */
    geom::Coordinate* getRightmostCoordinate();

    /** \brief
     * Creates the subgraph consisting of all edges reachable from
     * this node.
     *
     * Finds the edges in the graph and the rightmost coordinate.
     *
     * @param node a node to start the graph traversal from
     */
    void create(geomgraph::Node* node);

    void computeDepth(int outsideDepth);

    /** \brief
     * Find all edges whose depths indicates that they are in the
     * result area(s).
     *
     * Since we want polygon shells to be
     * oriented CW, choose dirEdges with the interior of the result
     * on the RHS.
     * Mark them as being in the result.
     * Interior Area edges are the result of dimensional collapses.
     * They do not form part of the result area boundary.
     */
    void findResultEdges();

    /** \brief
     * BufferSubgraphs are compared on the x-value of their rightmost
     * Coordinate.
     *
     * This defines a partial ordering on the graphs such that:
     *
     * g1 >= g2 <==> Ring(g2) does not contain Ring(g1)
     *
     * where Polygon(g) is the buffer polygon that is built from g.
     *
     * This relationship is used to sort the BufferSubgraphs so
     * that shells are guaranteed to
     * be built before holes.
     */
    int compareTo(BufferSubgraph*);

    /** \brief
     * Computes the envelope of the edges in the subgraph.
     * The envelope is cached after being computed.
     *
     * @return the envelope of the graph.
     */
    geom::Envelope* getEnvelope();
};

std::ostream& operator<< (std::ostream& os, const BufferSubgraph& bs);

// INLINES
inline geom::Coordinate*
BufferSubgraph::getRightmostCoordinate()
{
    return rightMostCoord;
}

inline std::vector<geomgraph::Node*>*
BufferSubgraph::getNodes()
{
    return &nodes;
}

inline std::vector<geomgraph::DirectedEdge*>*
BufferSubgraph::getDirectedEdges()
{
    return &dirEdgeList;
}

bool BufferSubgraphGT(BufferSubgraph* first, BufferSubgraph* second);

} // namespace geos::operation::buffer
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_BUFFER_BUFFERSUBGRAPH_H

