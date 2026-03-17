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
 * Last port: algorithm/distance/DiscreteHausdorffDistance.java 1.5 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/algorithm/distance/DiscreteHausdorffDistance.h>
#include <geos/geom/CoordinateSequence.h>

#include <typeinfo>
#include <cassert>

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm
namespace distance { // geos.algorithm.distance

void
DiscreteHausdorffDistance::MaxDensifiedByFractionDistanceFilter::filter_ro(
    const geom::CoordinateSequence& seq, size_t index)
{
    /*
     * This logic also handles skipping Point geometries
     */
    if(index == 0) {
        return;
    }

    const geom::Coordinate& p0 = seq.getAt(index - 1);
    const geom::Coordinate& p1 = seq.getAt(index);

    double delx = (p1.x - p0.x) / static_cast<double>(numSubSegs);
    double dely = (p1.y - p0.y) / static_cast<double>(numSubSegs);

    for(size_t i = 0; i < numSubSegs; ++i) {
        double x = p0.x + static_cast<double>(i) * delx;
        double y = p0.y + static_cast<double>(i) * dely;
        Coordinate pt(x, y);
        minPtDist.initialize();
        DistanceToPoint::computeDistance(geom, pt, minPtDist);
        maxPtDist.setMaximum(minPtDist);
    }

}

/* static public */
double
DiscreteHausdorffDistance::distance(const geom::Geometry& g0,
                                    const geom::Geometry& g1)
{
    DiscreteHausdorffDistance dist(g0, g1);
    return dist.distance();
}

/* static public */
double
DiscreteHausdorffDistance::distance(const geom::Geometry& g0,
                                    const geom::Geometry& g1,
                                    double densifyFrac)
{
    DiscreteHausdorffDistance dist(g0, g1);
    dist.setDensifyFraction(densifyFrac);
    return dist.distance();
}

/* private */
void
DiscreteHausdorffDistance::computeOrientedDistance(
    const geom::Geometry& discreteGeom,
    const geom::Geometry& geom,
    PointPairDistance& p_ptDist)
{
    // can't calculate distance with empty
    if (discreteGeom.isEmpty() || geom.isEmpty()) return;

    MaxPointDistanceFilter distFilter(geom);
    discreteGeom.apply_ro(&distFilter);
    p_ptDist.setMaximum(distFilter.getMaxPointDistance());

    if(densifyFrac > 0) {
        MaxDensifiedByFractionDistanceFilter fracFilter(geom,
                densifyFrac);
        discreteGeom.apply_ro(fracFilter);
        ptDist.setMaximum(fracFilter.getMaxPointDistance());
    }
}

} // namespace geos.algorithm.distance
} // namespace geos.algorithm
} // namespace geos

