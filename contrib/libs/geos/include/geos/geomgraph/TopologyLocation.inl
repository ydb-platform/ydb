/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geomgraph/TopologyLocation.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_TOPOLOGYLOCATION_INL
#define GEOS_GEOMGRAPH_TOPOLOGYLOCATION_INL

#include <geos/geom/Position.h>
#include <geos/geom/Location.h>

#include <cassert>

namespace geos {
namespace geomgraph { // geos.geomgraph

using geos::geom::Position;
using geos::geom::Location;

/*public*/
INLINE
TopologyLocation::TopologyLocation(Location on, Location left, Location right):
        locationSize(3)
{
    location[Position::ON] = on;
    location[Position::LEFT] = left;
    location[Position::RIGHT] = right;
}

/*public*/
INLINE
TopologyLocation::TopologyLocation(Location on):
        locationSize(1)
{
    location.fill(Location::NONE);
    location[Position::ON] = on;
}

/*public*/
INLINE
TopologyLocation::TopologyLocation(const TopologyLocation& gl)
        :
        location(gl.location),
        locationSize(gl.locationSize)
{
}

/*public*/
INLINE TopologyLocation&
TopologyLocation::operator= (const TopologyLocation& gl)
{
    location = gl.location;
    locationSize = gl.locationSize;
    return *this;
}

/*public*/
INLINE Location
TopologyLocation::get(size_t posIndex) const
{
    // should be an assert() instead ?
    if(posIndex < locationSize) {
        return location[posIndex];
    }
    return Location::NONE;
}

/*public*/
INLINE bool
TopologyLocation::isNull() const
{
    for(size_t i = 0; i < locationSize; ++i) {
        if(location[i] != Location::NONE) {
            return false;
        }
    }
    return true;
}

/*public*/
INLINE bool
TopologyLocation::isAnyNull() const
{
    for(size_t i = 0; i < locationSize; ++i) {
        if(location[i] == Location::NONE) {
            return true;
        }
    }
    return false;
}

/*public*/
INLINE bool
TopologyLocation::isEqualOnSide(const TopologyLocation& le, uint32_t locIndex) const
{
    return location[locIndex] == le.location[locIndex];
}

/*public*/
INLINE bool
TopologyLocation::isArea() const
{
    return locationSize > 1;
}

/*public*/
INLINE bool
TopologyLocation::isLine() const
{
    return locationSize == 1;
}

/*public*/
INLINE void
TopologyLocation::flip()
{
    if(locationSize <= 1) {
        return;
    }
    std::swap(location[Position::LEFT], location[Position::RIGHT]);
}

/*public*/
INLINE void
TopologyLocation::setAllLocations(Location locValue)
{
    location.fill(locValue);
}

/*public*/
INLINE void
TopologyLocation::setAllLocationsIfNull(Location locValue)
{
    for(size_t i = 0; i < locationSize; ++i) {
        if(location[i] == Location::NONE) {
            location[i] = locValue;
        }
    }
}

/*public*/
INLINE void
TopologyLocation::setLocation(size_t locIndex, Location locValue)
{
    location[locIndex] = locValue;
}

/*public*/
INLINE void
TopologyLocation::setLocation(Location locValue)
{
    setLocation(Position::ON, locValue);
}

/*public*/
INLINE const std::array<Location, 3>&
TopologyLocation::getLocations() const
{
    return location;
}

/*public*/
INLINE void
TopologyLocation::setLocations(Location on, Location left, Location right)
{
    assert(locationSize >= 3);
    location[Position::ON] = on;
    location[Position::LEFT] = left;
    location[Position::RIGHT] = right;
}

/*public*/
INLINE bool
TopologyLocation::allPositionsEqual(Location loc) const
{
    for(size_t i = 0; i < locationSize; ++i) {
        if(location[i] != loc) {
            return false;
        }
    }
    return true;
}

} // namespace geos.geomgraph
} // namespace geos

#endif
