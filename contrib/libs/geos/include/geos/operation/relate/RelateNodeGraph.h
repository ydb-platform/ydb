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
 * Last port: operation/relate/RelateNodeGraph.java rev. 1.11 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_RELATE_RELATENODEGRAPH_H
#define GEOS_OP_RELATE_RELATENODEGRAPH_H

#include <geos/export.h>
#include <geos/geomgraph/NodeMap.h>

#include <map>
#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
struct CoordinateLessThen;
}
namespace geomgraph {
//class EdgeEndStar;
class Node;
class GeometryGraph;
class EdgeEnd;
}
}


namespace geos {
namespace operation { // geos::operation
namespace relate { // geos::operation::relate

/** \brief
 * Implements the simple graph of Nodes and geomgraph::EdgeEnd which is all that is
 * required to determine topological relationships between Geometries.
 *
 * Also supports building a topological graph of a single Geometry, to
 * allow verification of valid topology.
 *
 * It is <b>not</b> necessary to create a fully linked
 * PlanarGraph to determine relationships, since it is sufficient
 * to know how the Geometries interact locally around the nodes.
 * In fact, this is not even feasible, since it is not possible to compute
 * exact intersection points, and hence the topology around those nodes
 * cannot be computed robustly.
 * The only Nodes that are created are for improper intersections;
 * that is, nodes which occur at existing vertices of the Geometries.
 * Proper intersections (e.g. ones which occur between the interior of
 * line segments)
 * have their topology determined implicitly, without creating a geomgraph::Node object
 * to represent them.
 *
 */
class GEOS_DLL RelateNodeGraph {

public:

    RelateNodeGraph();

    virtual ~RelateNodeGraph();

    geomgraph::NodeMap::container& getNodeMap();

    void build(geomgraph::GeometryGraph* geomGraph);

    void computeIntersectionNodes(geomgraph::GeometryGraph* geomGraph,
                                  int argIndex);

    void copyNodesAndLabels(geomgraph::GeometryGraph* geomGraph, int argIndex);

    void insertEdgeEnds(std::vector<geomgraph::EdgeEnd*>* ee);

private:

    geomgraph::NodeMap* nodes;

    RelateNodeGraph(const RelateNodeGraph&) = delete;
    RelateNodeGraph& operator=(const RelateNodeGraph&) = delete;

};


} // namespace geos:operation:relate
} // namespace geos:operation
} // namespace geos

#endif // GEOS_OP_RELATE_RELATENODEGRAPH_H
