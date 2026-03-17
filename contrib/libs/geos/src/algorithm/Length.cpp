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
 * Last port: algorithm/Length.java @ 2017-09-04
 *
 **********************************************************************/

#include <cmath>
#include <vector>

#include <geos/algorithm/Length.h>

namespace geos {
namespace algorithm { // geos.algorithm

/* public static */
double
Length::ofLine(const geom::CoordinateSequence* pts)
{
    // optimized for processing CoordinateSequences
    size_t n = pts->size();
    if(n <= 1) {
        return 0.0;
    }

    double len = 0.0;

    const geom::Coordinate& p = pts->getAt(0);
    double x0 = p.x;
    double y0 = p.y;

    for(size_t i = 1; i < n; i++) {
        const geom::Coordinate& pi = pts->getAt(i);
        double x1 = pi.x;
        double y1 = pi.y;
        double dx = x1 - x0;
        double dy = y1 - y0;

        len += std::sqrt(dx * dx + dy * dy);

        x0 = x1;
        y0 = y1;
    }
    return len;
}


} // namespace geos.algorithm
} //namespace geos

