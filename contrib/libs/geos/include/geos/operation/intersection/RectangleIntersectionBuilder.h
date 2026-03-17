/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2014 Mika Heiskanen <mika.heiskanen@fmi.fi>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_OP_INTERSECTION_RECTANGLEINTERSECTIONBUILDER_H
#define GEOS_OP_INTERSECTION_RECTANGLEINTERSECTIONBUILDER_H

#include <geos/export.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

#include <memory>
#include <list>
#include <vector>


// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Geometry;
class GeometryFactory;
class Polygon;
class LineString;
class Point;
}
namespace operation {
namespace intersection {
class Rectangle;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace intersection { // geos::operation::intersection

/**
 * \brief Rebuild geometries from subpaths left by clipping with a rectangle
 *
 * The RectangleIntersectionBuilder is used to maintain lists of polygons,
 * linestrings and points left from clipping a geom::Geometry with a Rectangle.
 * Once all clipping has been done, the class builds a valid geom::Geometry from
 * the components.
 *
 * @note This is a utility class needed by RectangleIntersection, and is not
 * intended for public use.
 */

class GEOS_DLL RectangleIntersectionBuilder {
    // Regular users are not supposed to use this utility class.
    friend class RectangleIntersection;

public:

    ~RectangleIntersectionBuilder();

private:

    /**
     * \brief Build the result geometry from partial results and clean up
     */
    std::unique_ptr<geom::Geometry> build();

    /**
     * \brief Build polygons from parts left by clipping one
     *
     * 1. Build exterior ring(s) from lines
     * 2. Attach polygons as holes to the exterior ring(s)
     */
    void reconnectPolygons(const Rectangle& rect);

    /**
     * Reconnect disjointed parts
     *
     * When we clip a LinearRing we may get multiple linestrings.
     * Often the first and last ones can be reconnected to simplify
     * output.
     *
     * Sample clip with a rectangle 0,0 --> 10,10 without reconnecting:
     *
     *   Input:   POLYGON ((5 10,0 0,10 0,5 10))
     *   Output:  MULTILINESTRING ((5 10,0 0),(10 0,5 10))
     *   Desired: LINESTRING (10 0,5 10,0 0)
     *
     * TODO: If there is a very sharp spike from inside the rectangle
     *       outside, and then back in, it is possible that the
     *       intersection points at the edge are equal. In this
     *       case we could reconnect the linestrings. The task is
     *       the same we're already doing for the 1st/last linestrings,
     *       we'd just do it for any adjacent pair as well.
     */
    void reconnect();

    void reverseLines();

    /**
     * Export parts to another container
     */
    void release(RectangleIntersectionBuilder& parts);

    // Adding Geometry components
    void add(geom::Polygon* g);
    void add(geom::LineString* g);
    void add(geom::Point* g);

    // Trivial methods
    bool empty() const;
    void clear();

    // Added components
    std::list<geom::Polygon*> polygons;
    std::list<geom::LineString*> lines;
    std::list<geom::Point*> points;

    /**
     * \brief Close a ring clockwise along rectangle edges
     *
     * Only the 4 corners and x1,y1 need to be considered. The possible
     * cases are:
     *
     *    x1,y1
     *    corner1 x1,y1
     *    corner1 corner2 x1,y1
     *    corner1 corner2 corner3 x1,y1
     *    corner1 corner2 corner3 corner4 x1,y1
     */
    void close_boundary(
        const Rectangle& rect,
        std::vector<geom::Coordinate>* ring,
        double x1, double y1,
        double x2, double y2);

    void close_ring(const Rectangle& rect, std::vector<geom::Coordinate>* ring);

    RectangleIntersectionBuilder(const geom::GeometryFactory& f)
        : _gf(f) {}

    const geom::GeometryFactory& _gf;

}; // class RectangleIntersectionBuilder

} // namespace geos::operation::intersection
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_INTERSECTION_RECTANGLEINTERSECTIONBUILDER_H
