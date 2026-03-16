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
 * Last port: geom/prep/PreparedLineStringIntersects.java r338 (JTS-1.12)
 *
 **********************************************************************/


#include <geos/algorithm/PointLocator.h>
#include <geos/geom/prep/PreparedLineString.h>
#include <geos/geom/prep/PreparedLineStringIntersects.h>
#include <geos/geom/util/ComponentCoordinateExtracter.h>
#include <geos/geom/Coordinate.h>
#include <geos/noding/SegmentStringUtil.h>
#include <geos/noding/FastSegmentSetIntersectionFinder.h>

using namespace geos::algorithm;
using namespace geos::geom::util;

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep

bool
PreparedLineStringIntersects::isAnyTestPointInTarget(const geom::Geometry* testGeom) const
{
    /*
     * This could be optimized by using the segment index on the lineal target.
     * However, it seems like the L/P case would be pretty rare in practice.
     */
    PointLocator  locator;

    geom::Coordinate::ConstVect coords;
    ComponentCoordinateExtracter::getCoordinates(*testGeom, coords);

    for(size_t i = 0, n = coords.size(); i < n; i++) {
        const geom::Coordinate& c = *(coords[i]);
        if(locator.intersects(c, &(prepLine.getGeometry()))) {
            return true;
        }
    }
    return false;
}

bool
PreparedLineStringIntersects::intersects(const geom::Geometry* g) const
{
    // If any segments intersect, obviously intersects = true
    noding::SegmentString::ConstVect lineSegStr;
    noding::SegmentStringUtil::extractSegmentStrings(g, lineSegStr);
    noding::FastSegmentSetIntersectionFinder* fssif = prepLine.getIntersectionFinder();
    bool segsIntersect = fssif->intersects(&lineSegStr); // prepLine.getIntersectionFinder()->intersects(lineSegStr);

    for(size_t i = 0, ni = lineSegStr.size(); i < ni; i++) {
        delete lineSegStr[ i ];
    }

    if(segsIntersect) {
        return true;
    }

    // For L/L case we are done
    if(g->getDimension() == 1) {
        return false;
    }

    // For L/A case, need to check for proper inclusion of the target in the test
    if(g->getDimension() == 2
            &&	prepLine.isAnyTargetComponentInTest(g)) {
        return true;
    }

    // For L/P case, need to check if any points lie on line(s)
    if(g->getDimension() == 0) {
        return isAnyTestPointInTarget(g);
    }

    return false;
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos

