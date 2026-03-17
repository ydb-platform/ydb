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

#ifndef GEOS_OP_RECTANGLE_INTERSECTION_H
#define GEOS_OP_RECTANGLE_INTERSECTION_H

#include <geos/export.h>

#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Point;
class MultiPoint;
class Polygon;
class MultiPolygon;
class LineString;
class MultiLineString;
class Geometry;
class GeometryCollection;
class GeometryFactory;
class CoordinateSequenceFactory;
}
namespace operation {
namespace intersection {
class Rectangle;
class RectangleIntersectionBuilder;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace intersection { // geos::operation::intersection

/**
 * \brief
 * Speed-optimized clipping of a [Geometry](@ref geom::Geometry) with a rectangle.
 *
 * Two different methods are provided. The first performs normal
 * clipping, the second clips the boundaries of polygons, not
 * the polygons themselves. In the first case a polygon will remain
 * a polygon or is completely cut out. In the latter case polygons
 * will be converted to polylines if any vertex is outside the clipping
 * rectangle, or will be cut out completely.
 *
 * The algorithm works best when the number of intersections is very low.
 * For example, if the geometry is completely to the left of the
 * clipping rectangle, only the x-coordinate of the geometry is ever
 * tested and is only compared with the x-coordinate of the left edge
 * of the rectangle. Hence clipping may be faster than calculating
 * the envelope of the geometry for trivial overlap tests.
 *
 * The input geometry must be valid. In particular all [LinearRings](@ref geom::LinearRing)
 * must be properly closed, or the algorithm may not terminate.
 *
 */
class GEOS_DLL RectangleIntersection {
public:

    /**
     * \brief Clip geometry with a rectangle.
     *
     * @param geom a [Geometry](@ref geom::Geometry)
     * @param rect a Rectangle
     * @return the clipped geometry
     * @return `NULL` if the geometry is outside the Rectangle
     */
    static std::unique_ptr<geom::Geometry> clip(const geom::Geometry& geom,
            const Rectangle& rect);

    /**
     * \brief Clip boundary of a geometry with a rectangle.
     *
     * Any polygon which intersects the rectangle will be converted to
     * a polyline or a multipolyline - including the holes.
     *
     * @param geom a [Geometry](@ref geom::Geometry)
     * @param rect a Rectangle
     * @return the clipped geometry
     * @return `NULL` if the geometry is outside the Rectangle
     */
    static std::unique_ptr<geom::Geometry> clipBoundary(const geom::Geometry& geom,
            const Rectangle& rect);

private:

    RectangleIntersection(const geom::Geometry& geom, const Rectangle& rect);

    std::unique_ptr<geom::Geometry> clipBoundary();

    std::unique_ptr<geom::Geometry> clip();

    const geom::Geometry& _geom;
    const Rectangle& _rect;
    const geom::GeometryFactory* _gf;
    const geom::CoordinateSequenceFactory* _csf;

    void clip_geom(const geom::Geometry* g,
                   RectangleIntersectionBuilder& parts,
                   const Rectangle& rect,
                   bool keep_polygons);

    void clip_point(const geom::Point* g,
                    RectangleIntersectionBuilder& parts,
                    const Rectangle& rect);

    void clip_multipoint(const geom::MultiPoint* g,
                         RectangleIntersectionBuilder& parts,
                         const Rectangle& rect);

    void clip_linestring(const geom::LineString* g,
                         RectangleIntersectionBuilder& parts,
                         const Rectangle& rect);

    void clip_multilinestring(const geom::MultiLineString* g,
                              RectangleIntersectionBuilder& parts,
                              const Rectangle& rect);

    void clip_polygon(const geom::Polygon* g,
                      RectangleIntersectionBuilder& parts,
                      const Rectangle& rect,
                      bool keep_polygons);

    void clip_multipolygon(const geom::MultiPolygon* g,
                           RectangleIntersectionBuilder& parts,
                           const Rectangle& rect,
                           bool keep_polygons);

    void clip_geometrycollection(
        const geom::GeometryCollection* g,
        RectangleIntersectionBuilder& parts,
        const Rectangle& rect,
        bool keep_polygons);

    void clip_polygon_to_linestrings(const geom::Polygon* g,
                                     RectangleIntersectionBuilder& parts,
                                     const Rectangle& rect);

    void clip_polygon_to_polygons(const geom::Polygon* g,
                                  RectangleIntersectionBuilder& parts,
                                  const Rectangle& rect);


    /**
     * \brief Clip geometry.
     *
     * Returns true if the geometry was fully inside, and does not output
     * anything to RectangleIntersectionBuilder.
     */
    bool clip_linestring_parts(const geom::LineString* gi,
                               RectangleIntersectionBuilder& parts,
                               const Rectangle& rect);

}; // class RectangleIntersection

} // namespace geos::operation::intersection
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_RECTANGLE_INTERSECTION_H
