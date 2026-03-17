/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/Quadrant.java rev. 1.8 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/geom/Quadrant.h>

#ifndef GEOS_INLINE
# include <geos/geom/Quadrant.inl>
#endif

using namespace geos::geom;

namespace geos {
namespace geom { // geos.geom

/* public static */
int
Quadrant::commonHalfPlane(int quad1, int quad2)
{
    // if quadrants are the same they do not determine a unique
    // common halfplane.
    // Simply return one of the two possibilities
    if(quad1 == quad2) {
        return quad1;
    }
    int diff = (quad1 - quad2 + 4) % 4;
    // if quadrants are not adjacent, they do not share a common halfplane
    if(diff == 2) {
        return -1;
    }
    //
    int min = (quad1 < quad2) ? quad1 : quad2;
    int max = (quad1 > quad2) ? quad1 : quad2;
    // for this one case, the righthand plane is NOT the minimum index;
    if(min == 0 && max == 3) {
        return 3;
    }
    // in general, the halfplane index is the minimum of the two
    // adjacent quadrants
    return min;
}

/* public static */
bool
Quadrant::isInHalfPlane(int quad, int halfPlane)
{
    if(halfPlane == SE) {
        return quad == SE || quad == SW;
    }
    return quad == halfPlane || quad == halfPlane + 1;
}


} // namespace geos.geom
} // namespace geos
