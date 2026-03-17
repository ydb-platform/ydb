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
 * Last port: algorithm/InteriorPointLine.java r317 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_INTERIORPOINTLINE_H
#define GEOS_ALGORITHM_INTERIORPOINTLINE_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class CoordinateSequence;
}
}


namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Computes a point in the interior of an linear geometry.
 *
 * <h2>Algorithm</h2>
 *
 * - Find an interior vertex which is closest to
 *   the centroid of the linestring.
 * - If there is no interior vertex, find the endpoint which is
 *   closest to the centroid.
 */
class GEOS_DLL InteriorPointLine {
public:

    InteriorPointLine(const geom::Geometry* g);
    //Coordinate* getInteriorPoint() const;

    bool getInteriorPoint(geom::Coordinate& ret) const;

private:

    bool hasInterior;

    geom::Coordinate centroid;

    double minDistance;

    geom::Coordinate interiorPoint;

    void addInterior(const geom::Geometry* geom);

    void addInterior(const geom::CoordinateSequence* pts);

    void addEndpoints(const geom::Geometry* geom);

    void addEndpoints(const geom::CoordinateSequence* pts);

    void add(const geom::Coordinate& point);

};

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_INTERIORPOINTLINE_H

