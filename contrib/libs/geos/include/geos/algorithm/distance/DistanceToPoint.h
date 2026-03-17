/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/distance/DistanceToPoint.java 1.1 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_DISTANCE_DISTANCETOPOINT_H
#define GEOS_ALGORITHM_DISTANCE_DISTANCETOPOINT_H

#include <geos/geom/LineSegment.h> // for composition

namespace geos {
namespace algorithm {
namespace distance {
class PointPairDistance;
}
}
namespace geom {
class Geometry;
class Coordinate;
class LineString;
class Polygon;
}
}

namespace geos {
namespace algorithm { // geos::algorithm
namespace distance { // geos::algorithm::distance

/**
 * Computes the Euclidean distance (L2 metric) from a Point to a Geometry.
 *
 * Also computes two points which are separated by the distance.
 */
class DistanceToPoint {
public:

    DistanceToPoint() {}

    static void computeDistance(const geom::Geometry& geom,
                                const geom::Coordinate& pt,
                                PointPairDistance& ptDist);

    static void computeDistance(const geom::LineString& geom,
                                const geom::Coordinate& pt,
                                PointPairDistance& ptDist);

    static void computeDistance(const geom::LineSegment& geom,
                                const geom::Coordinate& pt,
                                PointPairDistance& ptDist);

    static void computeDistance(const geom::Polygon& geom,
                                const geom::Coordinate& pt,
                                PointPairDistance& ptDist);

};

} // geos::algorithm::distance
} // geos::algorithm
} // geos

#endif // GEOS_ALGORITHM_DISTANCE_DISTANCETOPOINT_H

