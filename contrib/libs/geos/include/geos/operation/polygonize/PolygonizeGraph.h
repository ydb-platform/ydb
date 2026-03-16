/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/polygonize/PolygonizeGraph.java rev. 974
 *
 **********************************************************************/

#ifndef GEOS_OP_POLYGONIZE_POLYGONIZEGRAPH_H
#define GEOS_OP_POLYGONIZE_POLYGONIZEGRAPH_H

#include <geos/export.h>

#include <geos/planargraph/PlanarGraph.h> // for inheritance

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class LineString;
class GeometryFactory;
class Coordinate;
class CoordinateSequence;
}
namespace planargraph {
class Node;
class Edge;
class DirectedEdge;
}
namespace operation {
namespace polygonize {
class EdgeRing;
class PolygonizeDirectedEdge;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace polygonize { // geos::operation::polygonize


/** \brief
 * Represents a planar graph of edges that can be used to compute a
 * polygonization, and implements the algorithms to compute the
 * EdgeRings formed by the graph.
 *
 * The marked flag on DirectedEdge is used to indicate that a directed edge
 * has be logically deleted from the graph.
 *
 */
class GEOS_DLL PolygonizeGraph: public planargraph::PlanarGraph {

public:

    /**
     * \brief
     * Deletes all edges at a node
     */
    static void deleteAllEdges(planargraph::Node* node);

    /**
     * \brief
     * Create a new polygonization graph.
     */
    explicit PolygonizeGraph(const geom::GeometryFactory* newFactory);

    /**
     * \brief
     * Destroy a polygonization graph.
     */
    ~PolygonizeGraph() override;

    /**
     * \brief
     * Add a LineString forming an edge of the polygon graph.
     * @param line the line to add
     */
    void addEdge(const geom::LineString* line);

    /**
     * \brief
     * Computes the EdgeRings formed by the edges in this graph.
     *
     * @param edgeRingList : the EdgeRing found by the
     * 	polygonization process will be pushed here.
     *
     */
    void getEdgeRings(std::vector<EdgeRing*>& edgeRingList);

    /**
     * \brief
     * Finds and removes all cut edges from the graph.
     *
     * @param cutLines : the list of the LineString forming the removed
     *                   cut edges will be pushed here.
     *
     * TODO: document ownership of the returned LineStrings
     */
    void deleteCutEdges(std::vector<const geom::LineString*>& cutLines);

    /** \brief
     * Marks all edges from the graph which are "dangles".
     *
     * Dangles are which are incident on a node with degree 1.
     * This process is recursive, since removing a dangling edge
     * may result in another edge becoming a dangle.
     * In order to handle large recursion depths efficiently,
     * an explicit recursion stack is used
     *
     * @param dangleLines : the LineStrings that formed dangles will
     *                      be push_back'ed here
     */
    void deleteDangles(std::vector<const geom::LineString*>& dangleLines);

private:

    static int getDegreeNonDeleted(planargraph::Node* node);

    static int getDegree(planargraph::Node* node, long label);

    const geom::GeometryFactory* factory;

    planargraph::Node* getNode(const geom::Coordinate& pt);

    void computeNextCWEdges();

    /**
     * \brief
     * Convert the maximal edge rings found by the initial graph traversal
     * into the minimal edge rings required by JTS polygon topology rules.
     *
     * @param ringEdges
     * 	the list of start edges for the edgeRings to convert.
     *
     */
    void convertMaximalToMinimalEdgeRings(
        std::vector<PolygonizeDirectedEdge*>& ringEdges);

    /**
     * \brief
     * Finds all nodes in a maximal edgering
     * which are self-intersection nodes
     *
     * @param startDE
     * @param label
     * @param intNodes : intersection nodes found will be pushed here
     *                   the vector won't be cleared before pushing.
     */
    static void findIntersectionNodes(PolygonizeDirectedEdge* startDE,
                                      long label, std::vector<planargraph::Node*>& intNodes
                                     );

    /**
     * Finds and labels all edgerings in the graph.
     *
     * The edge rings are labeling with unique integers.
     * The labeling allows detecting cut edges.
     *
     * @param dirEdgesIn  a list of the DirectedEdges in the graph
     * @param dirEdgesOut each ring found will be pushed here
     */
    static void findLabeledEdgeRings(
        std::vector<planargraph::DirectedEdge*>& dirEdgesIn,
        std::vector<PolygonizeDirectedEdge*>& dirEdgesOut);

    static void label(std::vector<PolygonizeDirectedEdge*>& dirEdges, long label);
    static void label(std::vector<planargraph::DirectedEdge*>& dirEdges, long label);

    static void computeNextCWEdges(planargraph::Node* node);

    /**
     * \brief
     * Computes the next edge pointers going CCW around the given node,
     * for the given edgering label.
     * This algorithm has the effect of converting maximal edgerings
     * into minimal edgerings
     */
    static void computeNextCCWEdges(planargraph::Node* node, long label);

    EdgeRing* findEdgeRing(PolygonizeDirectedEdge* startDE);

    /* Tese are for memory management */
    std::vector<planargraph::Edge*> newEdges;
    std::vector<planargraph::DirectedEdge*> newDirEdges;
    std::vector<planargraph::Node*> newNodes;
    std::vector<EdgeRing*> newEdgeRings;
    std::vector<geom::CoordinateSequence*> newCoords;
};

} // namespace geos::operation::polygonize
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_POLYGONIZE_POLYGONIZEGRAPH_H
