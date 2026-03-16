/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/overlayng/InputGeometry.h>

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/*public*/
InputGeometry::InputGeometry(const Geometry* geomA, const Geometry* geomB)
    : geom({geomA, geomB})
    , isCollapsed({false, false})
{}

/*public*/
bool
InputGeometry::isSingle() const
{
    return geom[1] == nullptr;
}

/*public*/
int
InputGeometry::getDimension(int index) const
{
    if (geom[index] == nullptr)
        return -1;
    return geom[index]->getDimension();
}

/*public*/
const Geometry*
InputGeometry::getGeometry(int geomIndex) const
{
    return geom[geomIndex];
}

/*public*/
const Envelope*
InputGeometry::getEnvelope(int geomIndex) const
{
    return geom[geomIndex]->getEnvelopeInternal();
}

/*public*/
bool
InputGeometry::isEmpty(int geomIndex) const
{
    return geom[geomIndex]->isEmpty();
}

/*public*/
bool
InputGeometry::isArea(int geomIndex) const
{
    return geom[geomIndex] != nullptr && geom[geomIndex]->getDimension() == 2;
}

/**
* Gets the index of an input which is an area,
* if one exists.
* Otherwise returns -1.
* If both inputs are areas, returns the index of the first one (0).
*
* @return the index of an area input, or -1
*/

/*public*/
int
InputGeometry::getAreaIndex() const
{
    if (getDimension(0) == 2) return 0;
    if (getDimension(1) == 2) return 1;
    return -1;
}

/*public*/
bool
InputGeometry::isLine(int geomIndex) const
{
    return getDimension(geomIndex) == 1;
}

/*public*/
bool
InputGeometry::isAllPoints() const
{
    return getDimension(0) == 0
        && geom[1] != nullptr && getDimension(1) == 0;
}

  /*public*/
bool
InputGeometry::hasPoints() const
{
    return getDimension(0) == 0 || getDimension(1) == 0;
}

/**
* Tests if an input geometry has edges.
* This indicates that topology needs to be computed for them.
*
* @param geomIndex
* @return true if the input geometry has edges
*/
/*public*/
bool
InputGeometry::hasEdges(int geomIndex) const
{
    return geom[geomIndex] != nullptr && geom[geomIndex]->getDimension() > 0;
}

/**
* Determines the location within an area geometry.
* This allows disconnected edges to be fully
* located.
*
* @param geomIndex the index of the geometry
* @param pt the coordinate to locate
* @return the location of the coordinate
*
* @see Location
*/
/*public*/
Location
InputGeometry::locatePointInArea(int geomIndex, const Coordinate& pt)
{
    if (isCollapsed[geomIndex] || getGeometry(geomIndex)->isEmpty())
        return Location::EXTERIOR;

    PointOnGeometryLocator* ptLocator = getLocator(geomIndex);
    Location loc = ptLocator->locate(&pt);
    return loc;
}


/*private*/
PointOnGeometryLocator*
InputGeometry::getLocator(int geomIndex)
{
    if (geomIndex == 0) {
        if (ptLocatorA == nullptr)
            ptLocatorA.reset(new IndexedPointInAreaLocator(*getGeometry(geomIndex)));
        return ptLocatorA.get();
    }
    else {
        if (ptLocatorB == nullptr)
            ptLocatorB.reset(new IndexedPointInAreaLocator(*getGeometry(geomIndex)));
        return ptLocatorB.get();
    }
}


/*public*/
void
InputGeometry::setCollapsed(int geomIndex, bool isGeomCollapsed)
{
    isCollapsed[geomIndex] = isGeomCollapsed;
}









} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
