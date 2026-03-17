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
 * Last port: algorithm/distance/PointPairDistance.java 1.1 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_DISTANCE_POINTPAIRDISTANCE_H
#define GEOS_ALGORITHM_DISTANCE_POINTPAIRDISTANCE_H

#include <geos/constants.h> // for DoubleNotANumber
#include <geos/geom/Coordinate.h> // for inlines

#include <array>
#include <cassert>

namespace geos {
namespace algorithm { // geos::algorithm
namespace distance { // geos::algorithm::distance

/**
 * Contains a pair of points and the distance between them.
 * Provides methods to update with a new point pair with
 * either maximum or minimum distance.
 */
class PointPairDistance {
public:

    PointPairDistance()
        :
        distanceSquared(DoubleNotANumber),
        isNull(true)
    {}

    void
    initialize()
    {
        isNull = true;
    }

    void
    initialize(const geom::Coordinate& p0, const geom::Coordinate& p1)
    {
        pt[0] = p0;
        pt[1] = p1;
        distanceSquared = p0.distanceSquared(p1);
        isNull = false;
    }

    double
    getDistance() const
    {
        return std::sqrt(distanceSquared);
    }

    const std::array<geom::Coordinate, 2>&
    getCoordinates() const
    {
        return pt;
    }

    const geom::Coordinate&
    getCoordinate(size_t i) const
    {
        assert(i < pt.size());
        return pt[i];
    }

    void
    setMaximum(const PointPairDistance& ptDist)
    {
        setMaximum(ptDist.pt[0], ptDist.pt[1]);
    }

    void
    setMaximum(const geom::Coordinate& p0, const geom::Coordinate& p1)
    {
        if(isNull) {
            initialize(p0, p1);
            return;
        }
        double distSq = p0.distanceSquared(p1);
        if(distSq > distanceSquared) {
            initialize(p0, p1, distSq);
        }
    }

    void
    setMinimum(const PointPairDistance& ptDist)
    {
        setMinimum(ptDist.pt[0], ptDist.pt[1]);
    }

    void
    setMinimum(const geom::Coordinate& p0, const geom::Coordinate& p1)
    {
        if(isNull) {
            initialize(p0, p1);
            return;
        }
        double distSq = p0.distanceSquared(p1);
        if(distSq < distanceSquared) {
            initialize(p0, p1, distSq);
        }
    }

    bool
    getIsNull()
    {
        return isNull;
    }

private:

    /**
     * Initializes the points, avoiding recomputing the distance.
     * @param p0
     * @param p1
     * @param dist the distance between p0 and p1
     */
    void
    initialize(const geom::Coordinate& p0, const geom::Coordinate& p1,
               double distSquared)
    {
        pt[0] = p0;
        pt[1] = p1;
        distanceSquared = distSquared;
        isNull = false;
    }

    std::array<geom::Coordinate, 2> pt;

    double distanceSquared;

    bool isNull;
};

} // geos::algorithm::distance
} // geos::algorithm
} // geos

#endif // GEOS_ALGORITHM_DISTANCE_POINTPAIRDISTANCE_H

