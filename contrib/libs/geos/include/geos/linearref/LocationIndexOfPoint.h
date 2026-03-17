/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: linearref/LinearGeometryBuilder.java r466
 *
 **********************************************************************/

#ifndef GEOS_LINEARREF_LOCATIONINDEXOFPOINT_H
#define GEOS_LINEARREF_LOCATIONINDEXOFPOINT_H

#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/linearref/LinearLocation.h>

namespace geos {
namespace linearref { // geos::linearref

/** \brief
 * Computes the LinearLocation of the point on a linear [Geometry](@ref geom::Geometry)
 * nearest a given [Coordinate](@ref geom::Coordinate).
 *
 * The nearest point is not necessarily unique; this class always computes
 * the nearest point closest to the start of the geometry.
 */
class LocationIndexOfPoint {

private:
    const geom::Geometry* linearGeom;

    LinearLocation indexOfFromStart(const geom::Coordinate& inputPt, const LinearLocation* minIndex) const;

public:
    static LinearLocation indexOf(const geom::Geometry* linearGeom, const geom::Coordinate& inputPt);

    static LinearLocation indexOfAfter(const geom::Geometry* linearGeom, const geom::Coordinate& inputPt,
                                       const LinearLocation* minIndex);

    LocationIndexOfPoint(const geom::Geometry* linearGeom);

    /** \brief
     * Find the nearest location along a linear [Geometry](@ref geom::Geometry)
     * to a given point.
     *
     * @param inputPt the coordinate to locate
     * @return the location of the nearest point
     */
    LinearLocation indexOf(const geom::Coordinate& inputPt) const;

    /** \brief
     * Find the nearest LinearLocation along the linear [Geometry](@ref geom::Geometry)
     * to a given [Coordinate](@ref geom::Coordinate) after the specified minimum LinearLocation.
     *
     * If possible the location returned will be strictly greater than the
     * `minLocation`.
     * If this is not possible, the value returned will equal `minLocation`.
     * (An example where this is not possible is when `minLocation = [end of line]`).
     *
     * @param inputPt the coordinate to locate
     * @param minIndex the minimum location for the point location
     * @return the location of the nearest point
     */
    LinearLocation indexOfAfter(const geom::Coordinate& inputPt, const LinearLocation* minIndex) const;
};
}
}
#endif
