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
 * Last port: operation/valid/ConnectedInteriorTester.java rev. 1.15 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_CONNECTEDINTERIORTESTER_H
#define GEOS_OP_CONNECTEDINTERIORTESTER_H

#include <geos/export.h>

#include <geos/geom/Coordinate.h> // for composition
#include <geos/geom/GeometryFactory.h> // for GeometryFactory::Ptr

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
//class Coordinate;
class Geometry;
class CoordinateSequence;
class GeometryFactory;
class LineString;
}
namespace geomgraph {
class GeometryGraph;
class PlanarGraph;
class EdgeRing;
class DirectedEdge;
class EdgeEnd;
}
}

namespace geos {
namespace operation { // geos::operation
namespace valid { // geos::operation::valid

/** \brief
 * This class tests that the interior of an area Geometry
 * (Polygon or MultiPolygon)
 * is connected.
 *
 * An area Geometry is invalid if the interior is disconnected.
 * This can happen if:
 *
 * - one or more holes either form a chain touching the shell at two places
 * - one or more holes form a ring around a portion of the interior
 *
 * If an inconsistency if found the location of the problem
 * is recorded.
 */
class GEOS_DLL ConnectedInteriorTester {
public:
    ConnectedInteriorTester(geomgraph::GeometryGraph& newGeomGraph);
    ~ConnectedInteriorTester() = default;
    geom::Coordinate& getCoordinate();
    bool isInteriorsConnected();
    static const geom::Coordinate& findDifferentPoint(
        const geom::CoordinateSequence* coord,
        const geom::Coordinate& pt);

protected:

    void visitLinkedDirectedEdges(geomgraph::DirectedEdge* start);

private:

    geom::GeometryFactory::Ptr geometryFactory;

    geomgraph::GeometryGraph& geomGraph;

    /// Save a coordinate for any disconnected interior found
    /// the coordinate will be somewhere on the ring surrounding
    /// the disconnected interior
    geom::Coordinate disconnectedRingcoord;

    /// Used to track MaximalEdgeRings allocations
    std::vector<geomgraph::EdgeRing*> maximalEdgeRings;

    void setInteriorEdgesInResult(geomgraph::PlanarGraph& graph);


    /**
     * \brief
     * Form DirectedEdges in graph into Minimal EdgeRings.
     *
     * Minimal Edgerings must be used, because only they are
     * guaranteed to provide a correct isHole computation.
     *
     * @param minEdgeRings : newly allocated minimal edge rings will
     *                       be push_back'ed here.
     *                       deletion responsibility is left to caller.
     */
    void buildEdgeRings(std::vector<geomgraph::EdgeEnd*>* dirEdges,
                        std::vector<geomgraph::EdgeRing*>& minEdgeRings);

    /**
     * Mark all the edges for the edgeRings corresponding to the shells
     * of the input polygons.  Note only ONE ring gets marked for each shell.
     */
    void visitShellInteriors(const geom::Geometry* g, geomgraph::PlanarGraph& graph);

    void visitInteriorRing(const geom::LineString* ring, geomgraph::PlanarGraph& graph);

    /**
     * Check if any shell ring has an unvisited edge.
     * A shell ring is a ring which is not a hole and which has the interior
     * of the parent area on the RHS.
     * (Note that there may be non-hole rings with the interior on the LHS,
     * since the interior of holes will also be polygonized into CW rings
     * by the linkAllDirectedEdges() step)
     *
     * @return true if there is an unvisited edge in a non-hole ring
     */
    bool hasUnvisitedShellEdge(std::vector<geomgraph::EdgeRing*>* edgeRings);

    // Declare type as noncopyable
    ConnectedInteriorTester(const ConnectedInteriorTester& other) = delete;
    ConnectedInteriorTester& operator=(const ConnectedInteriorTester& rhs) = delete;
};

} // namespace geos::operation::valid
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_CONNECTEDINTERIORTESTER_H
