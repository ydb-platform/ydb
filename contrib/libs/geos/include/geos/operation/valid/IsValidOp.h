/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010 Sandro Santilli <strk@kbt.io>
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
 * Last port: operation/valid/IsValidOp.java r335 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_ISVALIDOP_H
#define GEOS_OP_ISVALIDOP_H

#include <geos/export.h>

#include <geos/operation/valid/TopologyValidationError.h> // for inlined destructor

// Forward declarations
namespace geos {
namespace util {
class TopologyValidationError;
}
namespace geom {
class CoordinateSequence;
class GeometryFactory;
class Geometry;
class Point;
class LinearRing;
class LineString;
class Polygon;
class GeometryCollection;
class MultiPolygon;
class MultiLineString;
}
namespace geomgraph {
class DirectedEdge;
class EdgeIntersectionList;
class PlanarGraph;
class GeometryGraph;
}
}

namespace geos {
namespace operation { // geos::operation
namespace valid { // geos::operation::valid

/** \brief
 * Implements the algorithsm required to compute the <code>isValid()</code>
 * method for [Geometrys](@ref geom::Geometry).
 */
class GEOS_DLL IsValidOp {
    friend class Unload;
private:
    /// the base Geometry to be validated
    const geom::Geometry* parentGeometry;

    bool isChecked;

    // CHECKME: should this really be a pointer ?
    TopologyValidationError* validErr;

    // This is the version using 'isChecked' flag
    void checkValid();

    void checkValid(const geom::Geometry* g);
    void checkValid(const geom::Point* g);
    void checkValid(const geom::LinearRing* g);
    void checkValid(const geom::LineString* g);
    void checkValid(const geom::Polygon* g);
    void checkValid(const geom::MultiPolygon* g);
    void checkValid(const geom::GeometryCollection* gc);
    void checkConsistentArea(geomgraph::GeometryGraph* graph);


    /**
     * Check that there is no ring which self-intersects
     * (except of course at its endpoints).
     * This is required by OGC topology rules (but not by other models
     * such as ESRI SDE, which allow inverted shells and exverted holes).
     *
     * @param graph the topology graph of the geometry
     */
    void checkNoSelfIntersectingRings(geomgraph::GeometryGraph* graph);

    /**
     * check that a ring does not self-intersect, except at its endpoints.
     * Algorithm is to count the number of times each node along edge
     * occurs.
     * If any occur more than once, that must be a self-intersection.
     */
    void checkNoSelfIntersectingRing(
        geomgraph::EdgeIntersectionList& eiList);

    void checkTooFewPoints(geomgraph::GeometryGraph* graph);

    /**
     * Test that each hole is inside the polygon shell.
     * This routine assumes that the holes have previously been tested
     * to ensure that all vertices lie on the shell or inside it.
     * A simple test of a single point in the hole can be used,
     * provide the point is chosen such that it does not lie on the
     * boundary of the shell.
     *
     * @param p the polygon to be tested for hole inclusion
     * @param graph a geomgraph::GeometryGraph incorporating the polygon
     */
    void checkHolesInShell(const geom::Polygon* p,
                           geomgraph::GeometryGraph* graph);

    /**
     * Tests that no hole is nested inside another hole.
     * This routine assumes that the holes are disjoint.
     * To ensure this, holes have previously been tested
     * to ensure that:
     *
     *  - they do not partially overlap
     *    (checked by <code>checkRelateConsistency</code>)
     *  - they are not identical
     *    (checked by <code>checkRelateConsistency</code>)
     *
     */
    void checkHolesNotNested(const geom::Polygon* p,
                             geomgraph::GeometryGraph* graph);

    /**
     * Tests that no element polygon is wholly in the interior of another
     * element polygon.
     *
     * Preconditions:
     *
     * - shells do not partially overlap
     * - shells do not touch along an edge
     * - no duplicate rings exist
     *
     * This routine relies on the fact that while polygon shells
     * may touch at one or more vertices, they cannot touch at
     * ALL vertices.
     */
    void checkShellsNotNested(const geom::MultiPolygon* mp,
                              geomgraph::GeometryGraph* graph);


    void checkConnectedInteriors(geomgraph::GeometryGraph& graph);

    void checkInvalidCoordinates(const geom::CoordinateSequence* cs);

    void checkInvalidCoordinates(const geom::Polygon* poly);

    void checkClosedRings(const geom::Polygon* poly);

    void checkClosedRing(const geom::LinearRing* ring);

    bool isSelfTouchingRingFormingHoleValid;

public:
    /** \brief
     * Find a point from the list of testCoords
     * that is NOT a node in the edge for the list of searchCoords
     *
     * @return the point found, or NULL if none found
     */
    static const geom::Coordinate* findPtNotNode(
        const geom::CoordinateSequence* testCoords,
        const geom::LinearRing* searchRing,
        const geomgraph::GeometryGraph* graph);

    /** \brief
     * Checks whether a coordinate is valid for processing.
     * Coordinates are valid iff their x and y coordinates are in the
     * range of the floating point representation.
     *
     * @param coord the coordinate to validate
     * @return `true` if the coordinate is valid
     */
    static bool isValid(const geom::Coordinate& coord);

    /** \brief
     * Tests whether a geom::Geometry is valid.
     *
     * @param geom the Geometry to test
     * @return `true` if the geometry is valid
     */
    static bool isValid(const geom::Geometry& geom);

    IsValidOp(const geom::Geometry* geom)
        :
        parentGeometry(geom),
        isChecked(false),
        validErr(nullptr),
        isSelfTouchingRingFormingHoleValid(false)
    {}

    /// TODO: validErr can't be a pointer!
    virtual
    ~IsValidOp()
    {
        delete validErr;
    }

    bool isValid();

    TopologyValidationError* getValidationError();

    /** \brief
     * Sets whether polygons using **Self-Touching Rings** to form
     * holes are reported as valid.
     *
     * If this flag is set, the following Self-Touching conditions
     * are treated as being valid:
     *
     * - the shell ring self-touches to create a hole touching the shell
     * - a hole ring self-touches to create two holes touching at a point
     *
     * The default (following the OGC SFS standard)
     * is that this condition is **not** valid (`false`).
     *
     * This does not affect whether Self-Touching Rings
     * disconnecting the polygon interior are considered valid
     * (these are considered to be **invalid** under the SFS,
     * and many other spatial models as well).
     * This includes "bow-tie" shells, which self-touch at a single point
     * causing the interior to be disconnected, and "C-shaped" holes which
     * self-touch at a single point causing an island to be formed.
     *
     * @param p_isValid states whether geometry with this condition is valid
     */
    void
    setSelfTouchingRingFormingHoleValid(bool p_isValid)
    {
        isSelfTouchingRingFormingHoleValid = p_isValid;
    }

};

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos

#endif // GEOS_OP_ISVALIDOP_H
