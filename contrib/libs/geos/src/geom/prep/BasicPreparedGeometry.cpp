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
 * Last port: geom/prep/BasicPreparedGeometry.java rev. 1.5 (JTS-1.10)
 *
 **********************************************************************/


#include <geos/geom/prep/BasicPreparedGeometry.h>
#include <geos/geom/Coordinate.h>
#include <geos/algorithm/PointLocator.h>
#include <geos/geom/util/ComponentCoordinateExtracter.h>
#include <geos/operation/distance/DistanceOp.h>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep

/*            *
 * protected: *
 *            */

void
BasicPreparedGeometry::setGeometry(const geom::Geometry* geom)
{
    baseGeom = geom;
    geom::util::ComponentCoordinateExtracter::getCoordinates(*baseGeom, representativePts);
}

bool
BasicPreparedGeometry::envelopesIntersect(const geom::Geometry* g) const
{
    if (g->getGeometryTypeId() == GEOS_POINT) {
        auto pt = g->getCoordinate();
        if (pt == nullptr) {
            return false;
        }
        return baseGeom->getEnvelopeInternal()->intersects(*pt);
    }

    return baseGeom->getEnvelopeInternal()->intersects(g->getEnvelopeInternal());
}

bool
BasicPreparedGeometry::envelopeCovers(const geom::Geometry* g) const
{
    if (g->getGeometryTypeId() == GEOS_POINT) {
        auto pt = g->getCoordinate();
        if (pt == nullptr) {
            return false;
        }
        return baseGeom->getEnvelopeInternal()->covers(pt);
    }

    return baseGeom->getEnvelopeInternal()->covers(g->getEnvelopeInternal());
}

/*
 * public:
 */
BasicPreparedGeometry::BasicPreparedGeometry(const Geometry* geom)
{
    setGeometry(geom);
}

bool
BasicPreparedGeometry::isAnyTargetComponentInTest(const geom::Geometry* testGeom) const
{
    algorithm::PointLocator locator;

    for(size_t i = 0, n = representativePts.size(); i < n; i++) {
        const geom::Coordinate& c = *(representativePts[i]);
        if(locator.intersects(c, testGeom)) {
            return true;
        }
    }
    return false;
}

bool
BasicPreparedGeometry::contains(const geom::Geometry* g) const
{
    return baseGeom->contains(g);
}

bool
BasicPreparedGeometry::containsProperly(const geom::Geometry* g)	const
{
    // since raw relate is used, provide some optimizations

    // short-circuit test
    if(! baseGeom->getEnvelopeInternal()->contains(g->getEnvelopeInternal())) {
        return false;
    }

    // otherwise, compute using relate mask
    return baseGeom->relate(g, "T**FF*FF*");
}

bool
BasicPreparedGeometry::coveredBy(const geom::Geometry* g) const
{
    return baseGeom->coveredBy(g);
}

bool
BasicPreparedGeometry::covers(const geom::Geometry* g) const
{
    return baseGeom->covers(g);
}

bool
BasicPreparedGeometry::crosses(const geom::Geometry* g) const
{
    return baseGeom->crosses(g);
}

bool
BasicPreparedGeometry::disjoint(const geom::Geometry* g)	const
{
    return ! intersects(g);
}

bool
BasicPreparedGeometry::intersects(const geom::Geometry* g) const
{
    return baseGeom->intersects(g);
}

bool
BasicPreparedGeometry::overlaps(const geom::Geometry* g)	const
{
    return baseGeom->overlaps(g);
}

bool
BasicPreparedGeometry::touches(const geom::Geometry* g) const
{
    return baseGeom->touches(g);
}

bool
BasicPreparedGeometry::within(const geom::Geometry* g) const
{
    return baseGeom->within(g);
}

std::unique_ptr<geom::CoordinateSequence>
BasicPreparedGeometry::nearestPoints(const geom::Geometry* g) const
{
    operation::distance::DistanceOp dist(baseGeom, g);
    return dist.nearestPoints();
}

double
BasicPreparedGeometry::distance(const geom::Geometry* g) const
{
    std::unique_ptr<geom::CoordinateSequence> coords = nearestPoints(g);
    if ( ! coords ) return std::numeric_limits<double>::infinity();
    return coords->getAt(0).distance( coords->getAt(1) );
}

std::string
BasicPreparedGeometry::toString()
{
    return baseGeom->toString();
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
