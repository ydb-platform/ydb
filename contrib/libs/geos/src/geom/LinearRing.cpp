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
 **********************************************************************
 *
 * Last port: geom/LinearRing.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/geom/LinearRing.h>
#include <geos/geom/Dimension.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/util/IllegalArgumentException.h>

#include <cassert>
#include <sstream>
#include <string>
#include <memory>

namespace geos {
namespace geom { // geos::geom

/*public*/
LinearRing::LinearRing(const LinearRing& lr): LineString(lr) {}

/*public*/
LinearRing::LinearRing(CoordinateSequence* newCoords,
                       const GeometryFactory* newFactory)
    :
    LineString(newCoords, newFactory)
{
    validateConstruction();
}

/*public*/
LinearRing::LinearRing(CoordinateSequence::Ptr && newCoords,
                       const GeometryFactory& newFactory)
        : LineString(std::move(newCoords), newFactory)
{
    validateConstruction();
}


void
LinearRing::validateConstruction()
{
    // Empty ring is valid
    if(points->isEmpty()) {
        return;
    }

    if(!LineString::isClosed()) {
        throw util::IllegalArgumentException(
            "Points of LinearRing do not form a closed linestring"
        );
    }

    if(points->getSize() < MINIMUM_VALID_SIZE) {
        std::ostringstream os;
        os << "Invalid number of points in LinearRing found "
           << points->getSize() << " - must be 0 or >= 4";
        throw util::IllegalArgumentException(os.str());
    }
}

int
LinearRing::getBoundaryDimension() const
{
    return Dimension::False;
}

bool
LinearRing::isClosed() const
{
    if(points->isEmpty()) {
        // empty LinearRings are closed by definition
        return true;
    }
    return LineString::isClosed();
}

std::string
LinearRing::getGeometryType() const
{
    return "LinearRing";
}

void
LinearRing::setPoints(const CoordinateSequence* cl)
{
    points = cl->clone();
}

GeometryTypeId
LinearRing::getGeometryTypeId() const
{
    return GEOS_LINEARRING;
}

std::unique_ptr<Geometry>
LinearRing::reverse() const
{
    if(isEmpty()) {
        return clone();
    }

    assert(points.get());
    auto seq = points->clone();
    CoordinateSequence::reverse(seq.get());
    assert(getFactory());
    return getFactory()->createLinearRing(std::move(seq));
}

} // namespace geos::geom
} // namespace geos
