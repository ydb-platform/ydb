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
 * Last port: operation/valid/ConsistentAreaTester.java rev. 1.14 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_CONSISTENTAREATESTER_H
#define GEOS_OP_CONSISTENTAREATESTER_H

#include <geos/export.h>

#include <geos/geom/Coordinate.h> // for composition
#include <geos/algorithm/LineIntersector.h> // for composition
#include <geos/operation/relate/RelateNodeGraph.h> // for composition

// Forward declarations
namespace geos {
namespace algorithm {
class LineIntersector;
}
namespace geomgraph {
class GeometryGraph;
}
namespace operation {
namespace relate {
class RelateNodeGraph;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace valid { // geos::operation::valid

/** \brief
 * Checks that a geomgraph::GeometryGraph representing an area
 * (a geom::Polygon or geom::MultiPolygon)
 * has consistent semantics for area geometries.
 * This check is required for any reasonable polygonal model
 * (including the OGC-SFS model, as well as models which allow ring
 * self-intersection at single points)
 *
 * Checks include:
 *
 *  - test for rings which properly intersect
 *    (but not for ring self-intersection, or intersections at vertices)
 *  - test for consistent labelling at all node points
 *    (this detects vertex intersections with invalid topology,
 *    i.e. where the exterior side of an edge lies in the interior of the area)
 *  - test for duplicate rings
 *
 * If an inconsistency is found the location of the problem
 * is recorded and is available to the caller.
 *
 */
class GEOS_DLL ConsistentAreaTester {
private:

    algorithm::LineIntersector li;

    /// Not owned
    geomgraph::GeometryGraph* geomGraph;

    relate::RelateNodeGraph nodeGraph;

    /// the intersection point found (if any)
    geom::Coordinate invalidPoint;

    /**
     * Check all nodes to see if their labels are consistent.
     * If any are not, return false
     */
    bool isNodeEdgeAreaLabelsConsistent();

public:

    /**
     * Creates a new tester for consistent areas.
     *
     * @param newGeomGraph the topology graph of the area geometry.
     *                     Caller keeps responsibility for its deletion
     */
    ConsistentAreaTester(geomgraph::GeometryGraph* newGeomGraph);

    ~ConsistentAreaTester() = default;

    /**
     * @return the intersection point, or <code>null</code>
     *         if none was found
     */
    geom::Coordinate& getInvalidPoint();

    /** \brief
     * Check all nodes to see if their labels are consistent with
     * area topology.
     *
     * @return <code>true</code> if this area has a consistent node
     *         labelling
     */
    bool isNodeConsistentArea();

    /**
     * Checks for two duplicate rings in an area.
     * Duplicate rings are rings that are topologically equal
     * (that is, which have the same sequence of points up to point order).
     * If the area is topologically consistent (determined by calling the
     * <code>isNodeConsistentArea</code>,
     * duplicate rings can be found by checking for EdgeBundles which contain
     * more than one geomgraph::EdgeEnd.
     * (This is because topologically consistent areas cannot have two rings sharing
     * the same line segment, unless the rings are equal).
     * The start point of one of the equal rings will be placed in
     * invalidPoint.
     *
     * @return true if this area Geometry is topologically consistent but has two duplicate rings
     */
    bool hasDuplicateRings();
};



} // namespace geos::operation::valid
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_CONSISTENTAREATESTER_H
