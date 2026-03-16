/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: operation/relate/RelateComputer.java rev. 1.24 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_RELATE_RELATECOMPUTER_H
#define GEOS_OP_RELATE_RELATECOMPUTER_H

#include <geos/export.h>

#include <geos/algorithm/PointLocator.h> // for RelateComputer composition
#include <geos/algorithm/LineIntersector.h> // for RelateComputer composition
#include <geos/geomgraph/NodeMap.h> // for RelateComputer composition
#include <geos/geom/Coordinate.h> // for RelateComputer composition
#include <geos/geom/IntersectionMatrix.h>

#include <vector>
#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace algorithm {
class BoundaryNodeRule;
}
namespace geom {
class Geometry;
}
namespace geomgraph {
class GeometryGraph;
class Edge;
class EdgeEnd;
class Node;
namespace index {
class SegmentIntersector;
}
}
}


namespace geos {
namespace operation { // geos::operation
namespace relate { // geos::operation::relate

/** \brief
 * Computes the topological relationship between two Geometries.
 *
 * RelateComputer does not need to build a complete graph structure to compute
 * the IntersectionMatrix.  The relationship between the geometries can
 * be computed by simply examining the labelling of edges incident on each node.
 *
 * RelateComputer does not currently support arbitrary GeometryCollections.
 * This is because GeometryCollections can contain overlapping Polygons.
 * In order to correct compute relate on overlapping Polygons, they
 * would first need to be noded and merged (if not explicitly, at least
 * implicitly).
 *
 */
class GEOS_DLL RelateComputer {
public:
    RelateComputer(std::vector<geomgraph::GeometryGraph*>* newArg);
    ~RelateComputer() = default;

    std::unique_ptr<geom::IntersectionMatrix> computeIM();
private:

    algorithm::LineIntersector li;

    algorithm::PointLocator ptLocator;

    /// the arg(s) of the operation
    std::vector<geomgraph::GeometryGraph*>* arg;

    geomgraph::NodeMap nodes;

    /// this intersection matrix will hold the results compute for the relate
    std::unique_ptr<geom::IntersectionMatrix> im;

    std::vector<geomgraph::Edge*> isolatedEdges;

    /// the intersection point found (if any)
    geom::Coordinate invalidPoint;

    void insertEdgeEnds(std::vector<geomgraph::EdgeEnd*>* ee);

    void computeProperIntersectionIM(
        geomgraph::index::SegmentIntersector* intersector,
        geom::IntersectionMatrix* imX);

    void copyNodesAndLabels(int argIndex);
    void computeIntersectionNodes(int argIndex);
    void labelIntersectionNodes(int argIndex);

    /**
     * If the Geometries are disjoint, we need to enter their dimension and
     * boundary dimension in the Ext rows in the IM
     */
    void computeDisjointIM(geom::IntersectionMatrix* imX,
                           const algorithm::BoundaryNodeRule& boundaryNodeRule);

    void labelNodeEdges();

    /**
     * update the IM with the sum of the IMs for each component
     */
    void updateIM(geom::IntersectionMatrix& imX);

    /**
     * Compute the IM entry for the intersection of the boundary
     * of a geometry with the Exterior.
     * This is the nominal dimension of the boundary
     * unless the boundary is empty, in which case it is {@link Dimension#FALSE}.
     * For linear geometries the Boundary Node Rule determines
     * whether the boundary is empty.
     *
     * @param geom the geometry providing the boundary
     * @param boundaryNodeRule  the Boundary Node Rule to use
     * @return the IM dimension entry
     */
    static int getBoundaryDim(const geom::Geometry& geom,
                              const algorithm::BoundaryNodeRule& boundaryNodeRule);

    /**
     * Processes isolated edges by computing their labelling and adding them
     * to the isolated edges list.
     * Isolated edges are guaranteed not to touch the boundary of the target
     * (since if they
     * did, they would have caused an intersection to be computed and hence would
     * not be isolated)
     */
    void labelIsolatedEdges(int thisIndex, int targetIndex);

    /**
     * Label an isolated edge of a graph with its relationship to the target
     * geometry.
     * If the target has dim 2 or 1, the edge can either be in the interior
     * or the exterior.
     * If the target has dim 0, the edge must be in the exterior
     */
    void labelIsolatedEdge(geomgraph::Edge* e, int targetIndex,
                           const geom::Geometry* target);

    /**
     * Isolated nodes are nodes whose labels are incomplete
     * (e.g. the location for one Geometry is null).
     * This is the case because nodes in one graph which don't intersect
     * nodes in the other are not completely labelled by the initial process
     * of adding nodes to the nodeList.
     * To complete the labelling we need to check for nodes that lie in the
     * interior of edges, and in the interior of areas.
     */
    void labelIsolatedNodes();

    /**
     * Label an isolated node with its relationship to the target geometry.
     */
    void labelIsolatedNode(geomgraph::Node* n, int targetIndex);
};


} // namespace geos:operation:relate
} // namespace geos:operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_RELATE_RELATECOMPUTER_H
