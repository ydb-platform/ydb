/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_POINTEXTRACTER_H
#define GEOS_GEOM_UTIL_POINTEXTRACTER_H

#include <geos/export.h>
#include <geos/geom/GeometryFilter.h>
#include <geos/geom/Point.h>
#include <vector>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/**
 * Extracts all the 0-dimensional (Point) components from a Geometry.
 */
class GEOS_DLL PointExtracter: public GeometryFilter {

public:
    /**
     * Returns the Point components from a single geometry.
     * If more than one geometry is to be processed, it is more
     * efficient to create a single PointExtracter filter instance
     * and pass it to multiple geometries.
     */
    static void getPoints(const Geometry& geom, Point::ConstVect& ret);

    /**
     * Constructs a PointExtracterFilter with a list in which
     * to store Points found.
     */
    PointExtracter(Point::ConstVect& newComps);

    void filter_rw(Geometry* geom) override;

    void filter_ro(const Geometry* geom) override;

private:

    Point::ConstVect& comps;

    // Declare type as noncopyable
    PointExtracter(const PointExtracter& other) = delete;
    PointExtracter& operator=(const PointExtracter& rhs) = delete;
};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
