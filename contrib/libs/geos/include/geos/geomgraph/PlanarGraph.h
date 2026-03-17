/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geomgraph/PlanarGraph.java r428 (JTS-1.12+)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_PLANARGRAPH_H
#define GEOS_GEOMGRAPH_PLANARGRAPH_H

#include <geos/export.h>
#include <map>
#include <vector>
#include <memory>

#include <geos/geom/Coordinate.h>
#include <geos/geomgraph/PlanarGraph.h>
#include <geos/geomgraph/NodeMap.h> // for typedefs
#include <geos/geomgraph/DirectedEdgeStar.h> // for inlines

#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace geomgraph {
class Edge;
class Node;
class EdgeEnd;
class NodeFactory;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

/**
 * \brief
 * Represents a directed graph which is embeddable in a planar surface.
 *
 * The computation of the IntersectionMatrix relies on the use of a structure
 * called a "topology graph".  The topology graph contains nodes and edges
 * corresponding to the nodes and line segments of a Geometry. Each
 * node and edge in the graph is labeled with its topological location
 * relative to the source geometry.
 *
 * Note that there is no requirement that points of self-intersection
 * be a vertex.
 * Thus to obtain a correct topology graph, Geometry objects must be
 * self-noded before constructing their graphs.
 *
 * Two fundamental operations are supported by topology graphs:
 *
 *  - Computing the intersections between all the edges and nodes of
 *    a single graph
 *  - Computing the intersections between the edges and nodes of two
 *    different graphs
 *
 */
class GEOS_DLL PlanarGraph {
public:

    /** \brief
     * For nodes in the collection (first..last),
     * link the DirectedEdges at the node that are in the result.
     *
     * This allows clients to link only a subset of nodes in the graph,
     * for efficiency (because they know that only a subset is of
     * interest).
     */
    template <typename It>
    static void
    linkResultDirectedEdges(It first, It last)
    // throw(TopologyException);
    {
        for(; first != last; ++first) {
            Node* node = *first;
            assert(node);

            EdgeEndStar* ees = node->getEdges();
            assert(ees);
            DirectedEdgeStar* des = dynamic_cast<DirectedEdgeStar*>(ees);
            assert(des);

            // this might throw an exception
            des->linkResultDirectedEdges();
        }
    }

    PlanarGraph(const NodeFactory& nodeFact);

    PlanarGraph();

    virtual ~PlanarGraph();

    virtual std::vector<Edge*>::iterator getEdgeIterator();

    virtual std::vector<EdgeEnd*>* getEdgeEnds();

    virtual bool isBoundaryNode(int geomIndex, const geom::Coordinate& coord);

    virtual void add(EdgeEnd* e);

    virtual NodeMap::iterator getNodeIterator();

    virtual void getNodes(std::vector<Node*>&);

    virtual Node* addNode(Node* node);

    virtual Node* addNode(const geom::Coordinate& coord);

    /**
     * @return the node if found; null otherwise
     */
    virtual Node* find(geom::Coordinate& coord);

    /** \brief
     * Add a set of edges to the graph.  For each edge two DirectedEdges
     * will be created.  DirectedEdges are NOT linked by this method.
     */
    virtual void addEdges(const std::vector<Edge*>& edgesToAdd);

    virtual void linkResultDirectedEdges();

    virtual void linkAllDirectedEdges();

    /** \brief
     * Returns the EdgeEnd which has edge e as its base edge
     * (MD 18 Feb 2002 - this should return a pair of edges)
     *
     * @return the edge, if found
     *    <code>null</code> if the edge was not found
     */
    virtual EdgeEnd* findEdgeEnd(Edge* e);

    /** \brief
     * Returns the edge whose first two coordinates are p0 and p1
     *
     * @return the edge, if found
     *    <code>null</code> if the edge was not found
     */
    virtual Edge* findEdge(const geom::Coordinate& p0,
                           const geom::Coordinate& p1);

    /** \brief
     * Returns the edge which starts at p0 and whose first segment is
     * parallel to p1
     *
     * @return the edge, if found
     *    <code>null</code> if the edge was not found
     */
    virtual Edge* findEdgeInSameDirection(const geom::Coordinate& p0,
                                          const geom::Coordinate& p1);

    virtual std::string printEdges();

    virtual NodeMap* getNodeMap();

protected:

    std::vector<Edge*>* edges;

    NodeMap* nodes;

    std::vector<EdgeEnd*>* edgeEndList;

    virtual void insertEdge(Edge* e);

private:

    /** \brief
     * The coordinate pairs match if they define line segments
     * lying in the same direction.
     *
     * E.g. the segments are parallel and in the same quadrant
     * (as opposed to parallel and opposite!).
     */
    bool matchInSameDirection(const geom::Coordinate& p0,
                              const geom::Coordinate& p1,
                              const geom::Coordinate& ep0,
                              const geom::Coordinate& ep1);

    PlanarGraph(const PlanarGraph&) = delete;
    PlanarGraph& operator=(const PlanarGraph&) = delete;
};



} // namespace geos.geomgraph
} // namespace geos

//#ifdef GEOS_INLINE
//# include "geos/geomgraph/PlanarGraph.inl"
//#endif

#endif // ifndef GEOS_GEOMGRAPH_PLANARGRAPH_H

