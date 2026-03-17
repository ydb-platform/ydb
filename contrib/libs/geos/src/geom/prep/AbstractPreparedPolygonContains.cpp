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
 * Last port: geom/prep/AbstractPreparedPolygonContains.java r388 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/geom/prep/AbstractPreparedPolygonContains.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/SegmentStringUtil.h>
#include <geos/noding/SegmentIntersectionDetector.h>
#include <geos/noding/FastSegmentSetIntersectionFinder.h>
#include <geos/algorithm/LineIntersector.h>
// std
#include <cstddef>

namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep
//
// private:
//
bool
AbstractPreparedPolygonContains::isProperIntersectionImpliesNotContainedSituation(const geom::Geometry* testGeom)
{
    // If the test geometry is polygonal we have the A/A situation.
    // In this case, a proper intersection indicates that
    // the Epsilon-Neighbourhood Exterior Intersection condition exists.
    // This condition means that in some small
    // area around the intersection point, there must exist a situation
    // where the interior of the test intersects the exterior of the target.
    // This implies the test is NOT contained in the target.
    if(testGeom->getGeometryTypeId() == geos::geom::GEOS_MULTIPOLYGON
            ||	testGeom->getGeometryTypeId() == geos::geom::GEOS_POLYGON) {
        return true;
    }

    // A single shell with no holes allows concluding that
    // a proper intersection implies not contained
    // (due to the Epsilon-Neighbourhood Exterior Intersection condition)
    if(isSingleShell(prepPoly->getGeometry())) {
        return true;
    }

    return false;
}


bool
AbstractPreparedPolygonContains::isSingleShell(const geom::Geometry& geom)
{
    // handles single-element MultiPolygons, as well as Polygons
    if(geom.getNumGeometries() != 1) {
        return false;
    }

    const geom::Geometry* g = geom.getGeometryN(0);
    const geom::Polygon* poly = dynamic_cast<const Polygon*>(g);
    assert(poly);

    std::size_t numHoles = poly->getNumInteriorRing();
    return (0 == numHoles);
}


void
AbstractPreparedPolygonContains::findAndClassifyIntersections(const geom::Geometry* geom)
{
    noding::SegmentString::ConstVect lineSegStr;
    noding::SegmentStringUtil::extractSegmentStrings(geom, lineSegStr);

    algorithm::LineIntersector li;

    noding::SegmentIntersectionDetector intDetector(&li);

    intDetector.setFindAllIntersectionTypes(true);
    prepPoly->getIntersectionFinder()->intersects(&lineSegStr, &intDetector);

    hasSegmentIntersection = intDetector.hasIntersection();
    hasProperIntersection = intDetector.hasProperIntersection();
    hasNonProperIntersection = intDetector.hasNonProperIntersection();

    for(std::size_t i = 0, ni = lineSegStr.size(); i < ni; i++) {
        delete lineSegStr[i];
    }
}


//
// protected:
//
bool
AbstractPreparedPolygonContains::eval(const geom::Geometry* geom)
{
    // Do point-in-poly tests first, since they are cheaper and may result
    // in a quick negative result.
    auto outermostLoc = getOutermostTestComponentLocation(geom);

    // Short-circuit for (Multi)Points
    if (geom->getDimension() == 0) {
        return evalPointTestGeom(geom, outermostLoc);
    }

    if (outermostLoc == Location::EXTERIOR) {
        // If a point of any test components does not lie in target,
        // result is false
        return false;
    }

    // Check if there is any intersection between the line segments
    // in target and test.
    // In some important cases, finding a proper interesection implies that the
    // test geometry is NOT contained.
    // These cases are:
    // - If the test geometry is polygonal
    // - If the target geometry is a single polygon with no holes
    // In both of these cases, a proper intersection implies that there
    // is some portion of the interior of the test geometry lying outside
    // the target, which means that the test is not contained.
    bool properIntersectionImpliesNotContained = isProperIntersectionImpliesNotContainedSituation(geom);

    // find all intersection types which exist
    findAndClassifyIntersections(geom);

    if(properIntersectionImpliesNotContained && hasProperIntersection) {
        return false;
    }

    // If all intersections are proper
    // (i.e. no non-proper intersections occur)
    // we can conclude that the test geometry is not contained in the target area,
    // by the Epsilon-Neighbourhood Exterior Intersection condition.
    // In real-world data this is likely to be by far the most common situation,
    // since natural data is unlikely to have many exact vertex segment intersections.
    // Thus this check is very worthwhile, since it avoid having to perform
    // a full topological check.
    //
    // (If non-proper (vertex) intersections ARE found, this may indicate
    // a situation where two shells touch at a single vertex, which admits
    // the case where a line could cross between the shells and still be wholely contained in them.
    if(hasSegmentIntersection && !hasNonProperIntersection) {
        return false;
    }

    // If there is a segment intersection and the situation is not one
    // of the ones above, the only choice is to compute the full topological
    // relationship.  This is because contains/covers is very sensitive
    // to the situation along the boundary of the target.
    if(hasSegmentIntersection) {
        return fullTopologicalPredicate(geom);
    }

    // This tests for the case where a ring of the target lies inside
    // a test polygon - which implies the exterior of the Target
    // intersects the interior of the Test, and hence the result is false
    if(geom->getGeometryTypeId() == geos::geom::GEOS_MULTIPOLYGON
            ||	geom->getGeometryTypeId() == geos::geom::GEOS_POLYGON) {
        // TODO: generalize this to handle GeometryCollections
        bool isTargetInTestArea = isAnyTargetComponentInAreaTest(geom, prepPoly->getRepresentativePoints());

        if(isTargetInTestArea) {
            return false;
        }
    }

    return true;
}

bool AbstractPreparedPolygonContains::evalPointTestGeom(const Geometry *geom, Location outermostLoc) {
    // If we had a point on the ourside of the polygon,
    // we aren't covered or contained.
    if (outermostLoc == Location::EXTERIOR) {
        return false;
    }

    // For the Covers predicate, we can return true
    // since no Points are on the exterior of the target
    // geometry.
    if (!requireSomePointInInterior) {
        return true;
    }

    // For the Contains predicate, we need to test if any
    // of those points lie in the interior of the target
    // geometry.
    if (outermostLoc == Location::INTERIOR) {
        return true;
    }

    if (geom->getNumGeometries() > 1) {
        // for MultiPoint, try to find at least one point
        // in interior
        return isAnyTestComponentInTargetInterior(geom);
    }

    return false;
}

//
// public:
//

} // geos::geom::prep
} // geos::geom
} // geos
