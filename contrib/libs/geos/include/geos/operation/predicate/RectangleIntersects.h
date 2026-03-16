/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/predicate/RectangleIntersects.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_PREDICATE_RECTANGLEINTERSECTS_H
#define GEOS_OP_PREDICATE_RECTANGLEINTERSECTS_H

#include <geos/export.h>

#include <geos/geom/Polygon.h> // for inlines

// Forward declarations
namespace geos {
namespace geom {
class Envelope;
//class Polygon;
}
}

namespace geos {
namespace operation { // geos::operation
namespace predicate { // geos::operation::predicate

/** \brief
 * Optimized implementation of the "intersects" spatial predicate
 * for cases where one Geometry is a rectangle.
 *
 * This class works for all input geometries, including
 * [GeometryCollections](@ref geom::GeometryCollection).
 *
 * As a further optimization,
 * this class can be used to test
 * many geometries against a single
 * rectangle in a slightly more efficient way.
 *
 */
class GEOS_DLL RectangleIntersects {

private:

    const geom::Polygon& rectangle;

    const geom::Envelope& rectEnv;

    // Declare type as noncopyable
    RectangleIntersects(const RectangleIntersects& other) = delete;
    RectangleIntersects& operator=(const RectangleIntersects& rhs) = delete;

public:

    /** \brief
     * Create a new intersects computer for a rectangle.
     *
     * @param newRect a rectangular geometry
     */
    RectangleIntersects(const geom::Polygon& newRect)
        :
        rectangle(newRect),
        rectEnv(*(newRect.getEnvelopeInternal()))
    {}

    bool intersects(const geom::Geometry& geom);

    /** \brief
     * Tests whether a rectangle intersects a given geometry.
     *
     * @param rectangle a rectangular Polygon
     * @param b a Geometry of any type
     * @return true if the geometries intersect
     */
    static bool
    intersects(const geom::Polygon& rectangle,
               const geom::Geometry& b)
    {
        RectangleIntersects rp(rectangle);
        return rp.intersects(b);
    }

};


} // namespace geos::operation::predicate
} // namespace geos::operation
} // namespace geos

#endif // ifndef GEOS_OP_PREDICATE_RECTANGLEINTERSECTS_H
