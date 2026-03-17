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
 * Last port: geom/Quadrant.java rev. 1.8 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_QUADRANT_INL
#define GEOS_GEOM_QUADRANT_INL

#include <geos/geom/Quadrant.h>
#include <geos/geom/Coordinate.h>
#include <geos/util/IllegalArgumentException.h>

#include <sstream>

namespace geos {
namespace geom {

/* public static */
INLINE int
Quadrant::quadrant(double dx, double dy)
{
    if(dx == 0.0 && dy == 0.0) {
        std::ostringstream s;
        s << "Cannot compute the quadrant for point ";
        s << "(" << dx << "," << dy << ")" << std::endl;
        throw util::IllegalArgumentException(s.str());
    }
    if(dx >= 0) {
        if(dy >= 0) {
            return NE;
        }
        else {
            return SE;
        }
    }
    else {
        if(dy >= 0) {
            return NW;
        }
        else {
            return SW;
        }
    }
}

/* public static */
INLINE int
Quadrant::quadrant(const geom::Coordinate& p0, const geom::Coordinate& p1)
{
    if(p1.x == p0.x && p1.y == p0.y) {
        throw util::IllegalArgumentException("Cannot compute the quadrant for two identical points " + p0.toString());
    }

    if(p1.x >= p0.x) {
        if(p1.y >= p0.y) {
            return NE;
        }
        else {
            return SE;
        }
    }
    else {
        if(p1.y >= p0.y) {
            return NW;
        }
        else {
            return SW;
        }
    }
}

/* public static */
INLINE bool
Quadrant::isOpposite(int quad1, int quad2)
{
    if(quad1 == quad2) {
        return false;
    }
    int diff = (quad1 - quad2 + 4) % 4;
    // if quadrants are not adjacent, they are opposite
    if(diff == 2) {
        return true;
    }
    return false;
}

/* public static */
INLINE bool
Quadrant::isNorthern(int quad)
{
    return quad == NE || quad == NW;
}

}
}

#endif

