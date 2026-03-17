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

#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/RayCrossingCounter.h>
#include <geos/algorithm/locate/SimplePointInAreaLocator.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Location.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineString.h>

#include <typeinfo>
#include <cassert>

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm
namespace locate { // geos.algorithm

/**
 * locate is the main location function.  It handles both single-element
 * and multi-element Geometries.  The algorithm for multi-element Geometries
 * is more complex, since it has to take into account the boundaryDetermination rule
 */
geom::Location
SimplePointInAreaLocator::locate(const Coordinate& p, const Geometry* geom)
{
    if(geom->isEmpty()) {
        return Location::EXTERIOR;
    }

    /*
     * Do a fast check against the geometry envelope first
     */
    if (! geom->getEnvelopeInternal()->intersects(p))
        return Location::EXTERIOR;

    return locateInGeometry(p, geom);
}

bool
SimplePointInAreaLocator::isContained(const Coordinate& p, const Geometry* geom)
{
    return Location::EXTERIOR != locate(p, geom);
}

geom::Location
SimplePointInAreaLocator::locateInGeometry(const Coordinate& p, const Geometry* geom)
{
    if (geom->getDimension() < 2) {
        return Location::EXTERIOR;
    }

    if (geom->getNumGeometries() == 1) {
        auto poly = dynamic_cast<const Polygon*>(geom->getGeometryN(0));
        if (poly) {
            return locatePointInPolygon(p, poly);
        }
        // Else it is a collection with a single element. Will be handled below.
    }
    for (size_t i = 0; i < geom->getNumGeometries(); i++) {
        const Geometry* gi = geom->getGeometryN(i);
        auto loc = locateInGeometry(p, gi);
        if(loc != Location::EXTERIOR) {
            return loc;
        }
    }

    return Location::EXTERIOR;
}

geom::Location
SimplePointInAreaLocator::locatePointInPolygon(const Coordinate& p, const Polygon* poly)
{
    if(poly->isEmpty()) {
        return Location::EXTERIOR;
    }
    if(!poly->getEnvelopeInternal()->contains(p)) {
        return Location::EXTERIOR;
    }
    const LineString* shell = poly->getExteriorRing();
    const CoordinateSequence* cl;
    cl = shell->getCoordinatesRO();
    Location shellLoc = PointLocation::locateInRing(p, *cl);
    if(shellLoc != Location::INTERIOR) {
        return shellLoc;
    }

    // now test if the point lies in or on the holes
    for(size_t i = 0, n = poly->getNumInteriorRing(); i < n; i++) {
        const LineString* hole = poly->getInteriorRingN(i);
        if(hole->getEnvelopeInternal()->contains(p)) {
            cl = hole->getCoordinatesRO();
            Location holeLoc = RayCrossingCounter::locatePointInRing(p, *cl);
            if(holeLoc == Location::BOUNDARY) {
                return Location::BOUNDARY;
            }
            if(holeLoc == Location::INTERIOR) {
                return Location::EXTERIOR;
            }
            // if in EXTERIOR of this hole, keep checking other holes
        }
    }
    return Location::INTERIOR;
}

} // namespace geos.algorithm.locate
} // namespace geos.algorithm
} // namespace geos
