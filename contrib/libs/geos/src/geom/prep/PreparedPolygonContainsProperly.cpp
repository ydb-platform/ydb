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
 * Last port: geom/prep/PreparedPolygonContainsProperly.java rev 1.5 (JTS-1.10)
 * (2007-12-12)
 *
 **********************************************************************/


#include <geos/geom/Geometry.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/geom/prep/PreparedPolygonContainsProperly.h>
#include <geos/geom/prep/PreparedPolygonPredicate.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/SegmentStringUtil.h>
#include <geos/noding/FastSegmentSetIntersectionFinder.h>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep
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
PreparedPolygonContainsProperly::containsProperly(const geom::Geometry* geom)
{
    // Do point-in-poly tests first, since they are cheaper and may result
    // in a quick negative result.
    // If a point of any test components does not lie in target,
    // result is false
    bool isAllInPrepGeomArea = isAllTestComponentsInTargetInterior(geom);
    if(!isAllInPrepGeomArea) {
        return false;
    }

    // If any segments intersect, result is false
    noding::SegmentString::ConstVect lineSegStr;
    noding::SegmentStringUtil::extractSegmentStrings(geom, lineSegStr);
    bool segsIntersect = prepPoly->getIntersectionFinder()->intersects(&lineSegStr);

    for(size_t i = 0, ni = lineSegStr.size(); i < ni; i++) {
        delete lineSegStr[ i ];
    }

    if(segsIntersect) {
        return false;
    }

    /*
     * Given that no segments intersect, if any vertex of the target
     * is contained in some test component.
     * the test is NOT properly contained.
     */
    if(geom->getGeometryTypeId() == geos::geom::GEOS_MULTIPOLYGON
            ||	geom->getGeometryTypeId() == geos::geom::GEOS_POLYGON) {
        // TODO: generalize this to handle GeometryCollections
        bool isTargetGeomInTestArea = isAnyTargetComponentInAreaTest(geom, prepPoly->getRepresentativePoints());
        if(isTargetGeomInTestArea) {
            return false;
        }
    }

    return true;
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
