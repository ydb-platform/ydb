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
 * Last port: geom/prep/PreparedPolygonPredicate.java rev. 1.4 (JTS-1.10)
 * (2007-12-12)
 *
 **********************************************************************/

#include <geos/geom/prep/PreparedPolygonPredicate.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/util/ComponentCoordinateExtracter.h>
#include <geos/geom/Location.h>
#include <geos/algorithm/locate/PointOnGeometryLocator.h>
#include <geos/algorithm/locate/SimplePointInAreaLocator.h>
// std
#include <cstddef>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep
//
// private:
//

//
// protected:
//
struct LocationMatchingFilter : public GeometryComponentFilter {
    explicit LocationMatchingFilter(algorithm::locate::PointOnGeometryLocator* locator, Location loc) :
        pt_locator(locator), test_loc(loc), found(false) {}

    algorithm::locate::PointOnGeometryLocator* pt_locator;
    const Location test_loc;
    bool found;

    void filter_ro(const Geometry* g) override {
        if (g->isEmpty())
            return;
        const Coordinate* pt = g->getCoordinate();
        const auto loc = pt_locator->locate(pt);

        if (loc == test_loc) {
            found = true;
        }
    }

    bool isDone() override {
        return found;
    }
};

struct LocationNotMatchingFilter : public GeometryComponentFilter {
    explicit LocationNotMatchingFilter(algorithm::locate::PointOnGeometryLocator* locator, Location loc) :
            pt_locator(locator), test_loc(loc), found(false) {}

    algorithm::locate::PointOnGeometryLocator* pt_locator;
    const Location test_loc;
    bool found;

    void filter_ro(const Geometry* g) override {
        if (g->isEmpty())
            return;
        const Coordinate* pt = g->getCoordinate();
        const auto loc = pt_locator->locate(pt);

        if (loc != test_loc) {
            found = true;
        }
    }

    bool isDone() override {
        return found;
    }
};

struct OutermostLocationFilter : public GeometryComponentFilter {
    explicit OutermostLocationFilter(algorithm::locate::PointOnGeometryLocator* locator) :
    pt_locator(locator),
    outermost_loc(geom::Location::NONE),
    done(false) {}

    algorithm::locate::PointOnGeometryLocator* pt_locator;
    Location outermost_loc;
    bool done;

    void filter_ro(const Geometry* g) override {
        if (g->isEmpty())
            return;
        const Coordinate* pt = g->getCoordinate();
        auto loc = pt_locator->locate(pt);

        if (outermost_loc == Location::NONE || outermost_loc == Location::INTERIOR) {
            outermost_loc = loc;
        } else if (loc == Location::EXTERIOR) {
            outermost_loc = loc;
            done = true;
        }
    }

    bool isDone() override {
        return done;
    }

    Location getOutermostLocation() {
        return outermost_loc;
    }
};

Location
PreparedPolygonPredicate::getOutermostTestComponentLocation(const geom::Geometry* testGeom) const
{
    OutermostLocationFilter filter(prepPoly->getPointLocator());
    testGeom->apply_ro(&filter);

    return filter.getOutermostLocation();
}

bool
PreparedPolygonPredicate::isAllTestComponentsInTargetInterior(
    const geom::Geometry* testGeom) const
{
    LocationNotMatchingFilter filter(prepPoly->getPointLocator(), geom::Location::INTERIOR);
    testGeom->apply_ro(&filter);

    return !filter.found;
}

bool
PreparedPolygonPredicate::isAnyTestComponentInTarget(
    const geom::Geometry* testGeom) const
{
    LocationNotMatchingFilter filter(prepPoly->getPointLocator(), geom::Location::EXTERIOR);
    testGeom->apply_ro(&filter);

    return filter.found;
}

bool
PreparedPolygonPredicate::isAnyTestComponentInTargetInterior(
    const geom::Geometry* testGeom) const
{
    LocationMatchingFilter filter(prepPoly->getPointLocator(), geom::Location::INTERIOR);
    testGeom->apply_ro(&filter);

    return filter.found;
}

bool
PreparedPolygonPredicate::isAnyTargetComponentInAreaTest(
    const geom::Geometry* testGeom,
    const geom::Coordinate::ConstVect* targetRepPts) const
{
    algorithm::locate::SimplePointInAreaLocator piaLoc(testGeom);

    for(std::size_t i = 0, ni = targetRepPts->size(); i < ni; i++) {
        const geom::Coordinate* pt = (*targetRepPts)[i];
        const Location loc = piaLoc.locate(pt);
        if(geom::Location::EXTERIOR != loc) {
            return true;
        }
    }

    return false;
}

//
// public:
//

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
