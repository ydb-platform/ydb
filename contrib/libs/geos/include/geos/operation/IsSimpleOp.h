/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009      Sandro Santilli <strk@kbt.io>
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
 * Last port: operation/IsSimpleOp.java rev. 1.22 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OPERATION_ISSIMPLEOP_H
#define GEOS_OPERATION_ISSIMPLEOP_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h> // for dtor visibility by unique_ptr (compos)

#include <map>
#include <memory> // for unique_ptr

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
class LineString;
class LinearRing;
class MultiLineString;
class MultiPoint;
class Geometry;
class Polygon;
class GeometryCollection;
struct CoordinateLessThen;
}
namespace geomgraph {
class GeometryGraph;
}
namespace operation {
class EndpointInfo;
}
}


namespace geos {
namespace operation { // geos.operation

/** \brief
 * Tests whether a Geometry is simple.
 *
 * In general, the SFS specification of simplicity follows the rule:
 *
 *  - A Geometry is simple if and only if the only self-intersections
 *    are at boundary points.
 *
 * This definition relies on the definition of boundary points.
 * The SFS uses the Mod-2 rule to determine which points are on the boundary of
 * lineal geometries, but this class supports using other
 * [BoundaryNodeRules](@ref algorithm::BoundaryNodeRule) as well.
 *
 * Simplicity is defined for each [Geometry](@ref geom::Geometry) subclass as follows:
 *
 *  - Valid polygonal geometries are simple by definition, so
 *    `isSimple` trivially returns true.
 *    (Hint: in order to check if a polygonal geometry has self-intersections,
 *    use geom::Geometry::isValid()).
 *
 *  - Linear geometries are simple iff they do not self-intersect at points
 *    other than boundary points.
 *    (Using the Mod-2 rule, this means that closed linestrings
 *    cannot be touched at their endpoints, since these are
 *    interior points, not boundary points).
 *
 *  - Zero-dimensional geometries (points) are simple iff they have no
 *    repeated points.
 *
 *  - Empty `Geometry`s are always simple
 *
 * @see algorithm::BoundaryNodeRule
 *
 */
class GEOS_DLL IsSimpleOp {

public:

    /** \brief
     * Creates a simplicity checker using the default
     * SFS Mod-2 Boundary Node Rule
     *
     * @deprecated use IsSimpleOp(Geometry)
     */
    IsSimpleOp();

    /** \brief
     * Creates a simplicity checker using the default
     * SFS Mod-2 Boundary Node Rule
     *
     * @param geom The geometry to test.
     *             Will store a reference: keep it alive.
     */
    IsSimpleOp(const geom::Geometry& geom);

    /** \brief
     * Creates a simplicity checker using a given
     * algorithm::BoundaryNodeRule
     *
     * @param geom the geometry to test
     * @param boundaryNodeRule the rule to use.
     */
    IsSimpleOp(const geom::Geometry& geom,
               const algorithm::BoundaryNodeRule& boundaryNodeRule);

    /** \brief
     * Tests whether the geometry is simple.
     *
     * @return true if the geometry is simple
     */
    bool isSimple();

    /** \brief
     * Gets a coordinate for the location where the geometry
     * fails to be simple (i.e. where it has a non-boundary self-intersection).
     *
     * {@link #isSimple} must be called before this method is called.
     *
     * @return a coordinate for the location of the non-boundary
     *         self-intersection. Ownership retained.
     * @return the null coordinate if the geometry is simple
     */
    const geom::Coordinate*
    getNonSimpleLocation() const
    {
        return nonSimpleLocation.get();
    }

    /** \brief
     * Reports whether a geom::LineString is simple.
     *
     * @param geom the lineal geometry to test
     * @return true if the geometry is simple
     *
     * @deprecated use isSimple()
     */
    bool isSimple(const geom::LineString* geom);

    /** \brief
     * Reports whether a geom::MultiLineString is simple.
     *
     * @param geom the lineal geometry to test
     * @return true if the geometry is simple
     *
     * @deprecated use isSimple()
     */
    bool isSimple(const geom::MultiLineString* geom);

    /** \brief
     * A MultiPoint is simple iff it has no repeated points
     *
     * @deprecated use isSimple()
     */
    bool isSimple(const geom::MultiPoint* mp);

    bool isSimpleLinearGeometry(const geom::Geometry* geom);

private:

    /**
     * For all edges, check if there are any intersections which are
     * NOT at an endpoint.
     * The Geometry is not simple if there are intersections not at
     * endpoints.
     */
    bool hasNonEndpointIntersection(geomgraph::GeometryGraph& graph);

    /**
     * Tests that no edge intersection is the endpoint of a closed line.
     * This ensures that closed lines are not touched at their endpoint,
     * which is an interior point according to the Mod-2 rule
     * To check this we compute the degree of each endpoint.
     * The degree of endpoints of closed lines
     * must be exactly 2.
     */
    bool hasClosedEndpointIntersection(geomgraph::GeometryGraph& graph);

    bool computeSimple(const geom::Geometry* geom);
    bool isSimplePolygonal(const geom::Geometry* geom);
    bool isSimpleGeometryCollection(const geom::GeometryCollection* col);

    /**
     * Add an endpoint to the map, creating an entry for it if none exists
     */
    void addEndpoint(std::map<const geom::Coordinate*, EndpointInfo*,
                     geom::CoordinateLessThen>& endPoints,
                     const geom::Coordinate* p, bool isClosed);

    bool isClosedEndpointsInInterior;

    bool isSimpleMultiPoint(const geom::MultiPoint& mp);

    const geom::Geometry* geom;

    std::unique_ptr<geom::Coordinate> nonSimpleLocation;
};

} // namespace geos.operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif
