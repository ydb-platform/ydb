/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2016  Shinichi SUGIYAMA <shin.sugi@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: original work
 *
 **********************************************************************/

#include <geos/algorithm/distance/DiscreteFrechetDistance.h>
#include <geos/geom/CoordinateSequence.h>

#include <geos/geom/Geometry.h>
#include <geos/geom/LineString.h>

#include <typeinfo>
#include <cassert>
#include <vector>
#include <algorithm>
#include <iostream>
using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm
namespace distance { // geos.algorithm.distance

/* static public */
double
DiscreteFrechetDistance::distance(const geom::Geometry& g0,
                                  const geom::Geometry& g1)
{
    DiscreteFrechetDistance dist(g0, g1);
    return dist.distance();
}

/* static public */
double
DiscreteFrechetDistance::distance(const geom::Geometry& g0,
                                  const geom::Geometry& g1,
                                  double densifyFrac)
{
    DiscreteFrechetDistance dist(g0, g1);
    dist.setDensifyFraction(densifyFrac);
    return dist.distance();
}

/* private */

geom::Coordinate
DiscreteFrechetDistance::getSegementAt(const CoordinateSequence& seq, size_t index)
{
    if(densifyFrac > 0.0) {
        size_t numSubSegs =  std::size_t(util::round(1.0 / densifyFrac));
        size_t i = index / numSubSegs;
        size_t j = index % numSubSegs;
        if(i >= seq.size() - 1) {
            return seq.getAt(seq.size() - 1);
        }
        const geom::Coordinate& p0 = seq.getAt(i);
        const geom::Coordinate& p1 = seq.getAt(i + 1);

        double delx = (p1.x - p0.x) / static_cast<double>(numSubSegs);
        double dely = (p1.y - p0.y) / static_cast<double>(numSubSegs);

        double x = p0.x + static_cast<double>(j) * delx;
        double y = p0.y + static_cast<double>(j) * dely;
        Coordinate pt(x, y);
        return pt;
    }
    else {
        return seq.getAt(index);
    }
}

PointPairDistance&
DiscreteFrechetDistance::getFrecheDistance(std::vector< std::vector<PointPairDistance> >& ca, size_t i, size_t j,
        const CoordinateSequence& p, const CoordinateSequence& q)
{
    PointPairDistance p_ptDist;
    if(! ca[i][j].getIsNull()) {
        return ca[i][j];
    }
    p_ptDist.initialize(getSegementAt(p, i), getSegementAt(q, j));
    if(i == 0 && j == 0) {
        ca[i][j] = p_ptDist;
    }
    else if(i > 0 && j == 0) {
        PointPairDistance nextDist = getFrecheDistance(ca, i - 1, 0, p, q);
        ca[i][j] = (nextDist.getDistance() > p_ptDist.getDistance()) ? nextDist : p_ptDist;
    }
    else if(i == 0 && j > 0) {
        PointPairDistance nextDist = getFrecheDistance(ca, 0, j - 1, p, q);
        ca[i][j] = (nextDist.getDistance() > p_ptDist.getDistance()) ? nextDist : p_ptDist;
    }
    else {
        PointPairDistance d1 = getFrecheDistance(ca, i - 1, j, p, q),
                          d2 = getFrecheDistance(ca, i - 1, j - 1, p, q),
                          d3 = getFrecheDistance(ca, i, j - 1, p, q);
        PointPairDistance& minDist = (d1.getDistance() < d2.getDistance()) ? d1 : d2;
        if(d3.getDistance() < minDist.getDistance()) {
            minDist = d3;
        }
        ca[i][j] = (minDist.getDistance() > p_ptDist.getDistance()) ? minDist : p_ptDist;
    }

    return ca[i][j];
}

void
DiscreteFrechetDistance::compute(
    const geom::Geometry& discreteGeom,
    const geom::Geometry& geom)
{
    auto lp = discreteGeom.getCoordinates();
    auto lq = geom.getCoordinates();
    size_t pSize, qSize;
    if(densifyFrac > 0) {
        size_t numSubSegs =  std::size_t(util::round(1.0 / densifyFrac));
        pSize = numSubSegs * (lp->size() - 1) + 1;
        qSize = numSubSegs * (lq->size() - 1) + 1;
    }
    else {
        pSize = lp->size();
        qSize = lq->size();
    }
    std::vector< std::vector<PointPairDistance> > ca(pSize, std::vector<PointPairDistance>(qSize));
    for(size_t i = 0; i < pSize; i++) {
        for(size_t j = 0; j < qSize; j++) {
            ca[i][j].initialize();
        }
    }
    ptDist = getFrecheDistance(ca, pSize - 1, qSize - 1, *lp, *lq);
}

} // namespace geos.algorithm.distance
} // namespace geos.algorithm
} // namespace geos
