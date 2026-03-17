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
 * Last port: geom/prep/PreparedPolygonIntersects.java rev 1.6 (JTS-1.10)
 * (2007-12-12)
 *
 **********************************************************************/


#include <geos/geom/prep/PreparedPolygonIntersects.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/prep/PreparedPolygonPredicate.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/SegmentStringUtil.h>
#include <geos/noding/FastSegmentSetIntersectionFinder.h>

namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep
//
// private:
//

//
// protected:
//

//
// public:
//
bool
PreparedPolygonIntersects::intersects(const geom::Geometry* geom)
{
    // Do point-in-poly tests first, since they are cheaper and may result
    // in a quick positive result.
    // If a point of any test components lie in target, result is true
    bool isInPrepGeomArea = isAnyTestComponentInTarget(geom);
    if(isInPrepGeomArea) {
        return true;
    }

    if(geom->isPuntal()) {
        // point-in-poly failed, no way there can be an intersection
        // (NOTE: isAnyTestComponentInTarget also checks for boundary)
        return false;
    }

    // If any segments intersect, result is true
    noding::SegmentString::ConstVect lineSegStr;
    noding::SegmentStringUtil::extractSegmentStrings(geom, lineSegStr);
    bool segsIntersect = prepPoly->getIntersectionFinder()->intersects(&lineSegStr);

    for(size_t i = 0, ni = lineSegStr.size(); i < ni; i++) {
        delete lineSegStr[ i ];
    }

    if(segsIntersect) {
        return true;
    }

    // If the test has dimension = 2 as well, it is necessary to
    // test for proper inclusion of the target.
    // Since no segments intersect, it is sufficient to test representative points.
    if(geom->getDimension() == 2) {
        // TODO: generalize this to handle GeometryCollections
        bool isPrepGeomInArea = isAnyTargetComponentInAreaTest(geom, prepPoly->getRepresentativePoints());
        if(isPrepGeomInArea) {
            return true;
        }
    }

    return false;
}

} // geos::geom::prep
} // geos::geom
} // geos
