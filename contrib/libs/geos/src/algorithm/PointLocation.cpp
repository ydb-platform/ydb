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
 * Last port: algorithm/PointLocation.java @ 2017-09-04
 *
 **********************************************************************/

#include <cmath>
#include <vector>

#include <geos/algorithm/LineIntersector.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/RayCrossingCounter.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Location.h>
#include <geos/util/IllegalArgumentException.h>

namespace geos {
namespace algorithm { // geos.algorithm

/* public static */
bool
PointLocation::isOnLine(const geom::Coordinate& p, const geom::CoordinateSequence* pt)
{
    size_t ptsize = pt->getSize();
    if(ptsize == 0) {
        return false;
    }

    const geom::Coordinate* pp = &(pt->getAt(0));
    for(size_t i = 1; i < ptsize; ++i) {
        const geom::Coordinate& p1 = pt->getAt(i);
        if(LineIntersector::hasIntersection(p, *pp, p1)) {
            return true;
        }
        pp = &p1;
    }
    return false;
}

/* public static */
bool
PointLocation::isInRing(const geom::Coordinate& p,
                        const std::vector<const geom::Coordinate*>& ring)
{
    return PointLocation::locateInRing(p, ring) != geom::Location::EXTERIOR;
}

/* public static */
bool
PointLocation::isInRing(const geom::Coordinate& p,
                        const geom::CoordinateSequence* ring)
{
    return PointLocation::locateInRing(p, *ring) != geom::Location::EXTERIOR;
}

/* public static */
geom::Location
PointLocation::locateInRing(const geom::Coordinate& p,
                            const std::vector<const geom::Coordinate*>& ring)
{
    return RayCrossingCounter::locatePointInRing(p, ring);
}

/* public static */
geom::Location
PointLocation::locateInRing(const geom::Coordinate& p,
                            const geom::CoordinateSequence& ring)
{
    return RayCrossingCounter::locatePointInRing(p, ring);
}


} // namespace geos.algorithm
} // namespace geos

