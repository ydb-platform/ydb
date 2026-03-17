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
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_LOCATE_POINTONGEOMETRYLOCATOR_H
#define GEOS_ALGORITHM_LOCATE_POINTONGEOMETRYLOCATOR_H

#include <geos/geom/Location.h>

namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace algorithm { // geos::algorithm
namespace locate { // geos::algorithm::locate

/** \brief
 * An interface for classes which determine the [Location](@ref geom::Location) of
 * points in [Polygon](@ref geom::Polygon) or [MultiPolygon](@ref geom::MultiPolygon) geometries.
 *
 * @author Martin Davis
 */
class GEOS_DLL PointOnGeometryLocator {
private:
protected:
public:
    virtual
    ~PointOnGeometryLocator()
    { }

    /**
     * Determines the [Location](@ref geom::Location) of a point in an areal [Geometry](@ref geom::Geometry).
     *
     * @param p the point to test
     * @return the location of the point in the geometry
     */
    virtual geom::Location locate(const geom::Coordinate* /*const*/ p) = 0;
};

} // geos::algorithm::locate
} // geos::algorithm
} // geos

#endif // GEOS_ALGORITHM_LOCATE_POINTONGEOMETRYLOCATOR_H
