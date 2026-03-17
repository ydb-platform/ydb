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
 **********************************************************************/

#include <geos/algorithm/SimplePointInRing.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/geom/LinearRing.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace algorithm { // geos.algorithm

SimplePointInRing::SimplePointInRing(geom::LinearRing* ring)
{
    pts = ring->getCoordinatesRO();
}

bool
SimplePointInRing::isInside(const geom::Coordinate& pt)
{
    return PointLocation::isInRing(pt, pts);
}

} // namespace geos.algorithm
} // namespace geos
