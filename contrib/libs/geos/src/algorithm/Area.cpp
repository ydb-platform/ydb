/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2018 Paul Ramsey <pramsey@cleverlephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/Area.java @ 2017-09-04
 *
 **********************************************************************/

#include <cmath>
#include <vector>

#include <geos/algorithm/Area.h>

namespace geos {
namespace algorithm { // geos.algorithm

/* public static */
double
Area::ofRing(const std::vector<geom::Coordinate>& ring)
{
    return std::abs(ofRingSigned(ring));
}

/* public static */
double
Area::ofRing(const geom::CoordinateSequence* ring)
{
    return std::abs(ofRingSigned(ring));
}

/* public static */
double
Area::ofRingSigned(const std::vector<geom::Coordinate>& ring)
{
    size_t rlen = ring.size();
    if(rlen < 3) {
        return 0.0;
    }

    double sum = 0.0;
    /*
     * Based on the Shoelace formula.
     * http://en.wikipedia.org/wiki/Shoelace_formula
     */
    double x0 = ring[0].x;
    for(size_t i = 1; i < rlen - 1; i++) {
        double x = ring[i].x - x0;
        double y1 = ring[i + 1].y;
        double y2 = ring[i - 1].y;
        sum += x * (y2 - y1);
    }
    return sum / 2.0;
}

/* public static */
double
Area::ofRingSigned(const geom::CoordinateSequence* ring)
{
    size_t n = ring->size();
    if(n < 3) {
        return 0.0;
    }
    /*
     * Based on the Shoelace formula.
     * http://en.wikipedia.org/wiki/Shoelace_formula
     */
    geom::Coordinate p0, p1, p2;
    p1 = ring->getAt(0);
    p2 = ring->getAt(1);
    double x0 = p1.x;
    p2.x -= x0;
    double sum = 0.0;
    for(size_t i = 1; i < n - 1; i++) {
        p0.y = p1.y;
        p1.x = p2.x;
        p1.y = p2.y;
        p2 = ring->getAt(i + 1);
        p2.x -= x0;
        sum += p1.x * (p0.y - p2.y);
    }
    return sum / 2.0;
}



} // namespace geos.algorithm
} //namespace geos

