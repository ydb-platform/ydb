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
 * Last port: geom/prep/PreparedPolygon.java rev 1.7 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/geom/Polygon.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/geom/prep/PreparedPolygonContains.h>
#include <geos/geom/prep/PreparedPolygonContainsProperly.h>
#include <geos/geom/prep/PreparedPolygonCovers.h>
#include <geos/geom/prep/PreparedPolygonDistance.h>
#include <geos/geom/prep/PreparedPolygonIntersects.h>
#include <geos/geom/prep/PreparedPolygonPredicate.h>
#include <geos/noding/FastSegmentSetIntersectionFinder.h>
#include <geos/noding/SegmentStringUtil.h>
#include <geos/operation/predicate/RectangleContains.h>
#include <geos/operation/predicate/RectangleIntersects.h>
#include <geos/algorithm/locate/PointOnGeometryLocator.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
// std
#include <cstddef>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep
//
// public:
//
PreparedPolygon::PreparedPolygon(const geom::Geometry* geom)
    : BasicPreparedGeometry(geom)
{
    isRectangle = getGeometry().isRectangle();
}

PreparedPolygon::~PreparedPolygon()
{
    for(std::size_t i = 0, ni = segStrings.size(); i < ni; i++) {
        delete segStrings[ i ];
    }
}


noding::FastSegmentSetIntersectionFinder*
PreparedPolygon::
getIntersectionFinder() const
{
    if(! segIntFinder) {
        noding::SegmentStringUtil::extractSegmentStrings(&getGeometry(), segStrings);
        segIntFinder.reset(new noding::FastSegmentSetIntersectionFinder(&segStrings));
    }
    return segIntFinder.get();
}

algorithm::locate::PointOnGeometryLocator*
PreparedPolygon::
getPointLocator() const
{
    if(! ptOnGeomLoc) {
        ptOnGeomLoc.reset(new algorithm::locate::IndexedPointInAreaLocator(getGeometry()));
    }

    return ptOnGeomLoc.get();
}

bool
PreparedPolygon::
contains(const geom::Geometry* g) const
{
    // short-circuit test
    if(!envelopeCovers(g)) {
        return false;
    }

    // optimization for rectangles
    if(isRectangle) {
        geom::Geometry const& geom = getGeometry();
        geom::Polygon const& poly = dynamic_cast<geom::Polygon const&>(geom);

        return operation::predicate::RectangleContains::contains(poly, *g);
    }

    return PreparedPolygonContains::contains(this, g);
}

bool
PreparedPolygon::
containsProperly(const geom::Geometry* g) const
{
    // short-circuit test
    if(!envelopeCovers(g)) {
        return false;
    }

    return PreparedPolygonContainsProperly::containsProperly(this, g);
}

bool
PreparedPolygon::
covers(const geom::Geometry* g) const
{
    // short-circuit test
    if(!envelopeCovers(g)) {
        return false;
    }

    // optimization for rectangle arguments
    if(isRectangle) {
        return true;
    }

    return PreparedPolygonCovers::covers(this, g);
}

bool
PreparedPolygon::
intersects(const geom::Geometry* g) const
{
    // envelope test
    if(!envelopesIntersect(g)) {
        return false;
    }

    // optimization for rectangles
    if(isRectangle) {
        geom::Geometry const& geom = getGeometry();
        geom::Polygon const& poly = dynamic_cast<geom::Polygon const&>(geom);

        return operation::predicate::RectangleIntersects::intersects(poly, *g);
    }

    return PreparedPolygonIntersects::intersects(this, g);
}

/* public */
operation::distance::IndexedFacetDistance*
PreparedPolygon::
getIndexedFacetDistance() const
{
    if(! indexedDistance ) {
        indexedDistance.reset(new operation::distance::IndexedFacetDistance(&getGeometry()));
    }
    return indexedDistance.get();
}

double
PreparedPolygon::distance(const geom::Geometry* g) const
{
    return PreparedPolygonDistance::distance(*this, g);
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
