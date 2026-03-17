/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/validate/FuzzyPointLocator.java rev. 1.1 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/overlay/validate/FuzzyPointLocator.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Point.h> // for Point upcast
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Location.h> // for Location::Value enum
#include <geos/util.h>

#include <cassert>
#include <functional>
#include <vector>
#include <sstream>
#include <memory> // for unique_ptr

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#define COMPUTE_Z 1
#define USE_ELEVATION_MATRIX 1
#define USE_INPUT_AVGZ 0

using namespace geos::geom;
using namespace geos::algorithm;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay
namespace validate { // geos.operation.overlay.validate

FuzzyPointLocator::FuzzyPointLocator(const geom::Geometry& geom,
                                     double nTolerance)
    :
    g(geom),
    tolerance(nTolerance),
    ptLocator(),
    linework(extractLineWork(g))
{
}

/*private*/
std::unique_ptr<Geometry>
FuzzyPointLocator::extractLineWork(const geom::Geometry& geom)
{
    ::geos::ignore_unused_variable_warning(geom);

    std::vector<std::unique_ptr<Geometry>> lineGeoms;

    for(size_t i = 0, n = g.getNumGeometries(); i < n; ++i) {
        const Geometry* gComp = g.getGeometryN(i);

        // only get linework for polygonal components
        if(gComp->getDimension() == Dimension::A) {
            lineGeoms.push_back(gComp->getBoundary());
        }
    }
    return g.getFactory()->buildGeometry(std::move(lineGeoms));
}

/*private*/
std::unique_ptr<Geometry>
FuzzyPointLocator::getLineWork(const geom::Geometry& geom)
{
    ::geos::ignore_unused_variable_warning(geom);

    std::vector<std::unique_ptr<Geometry>> lineGeoms;
    for(size_t i = 0, n = g.getNumGeometries(); i < n; ++i) {
        const Geometry* gComp = g.getGeometryN(i);

        if(gComp->getDimension() == 2) {
            lineGeoms.push_back(gComp->getBoundary());
        } else {
            lineGeoms.push_back(gComp->clone());
        }
    }

    return g.getFactory()->buildGeometry(std::move(lineGeoms));
}

/* public */
Location
FuzzyPointLocator::getLocation(const Coordinate& pt)
{
    std::unique_ptr<Geometry> point(g.getFactory()->createPoint(pt));

    double dist = linework->distance(point.get());

    // if point is close to boundary, it is considered
    // to be on the boundary
    if(dist < tolerance) {
        return Location::BOUNDARY;
    }

    // now we know point must be clearly inside or outside geometry,
    // so return actual location value
    return ptLocator.locate(pt, &g);
}

} // namespace geos.operation.overlay.validate
} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

