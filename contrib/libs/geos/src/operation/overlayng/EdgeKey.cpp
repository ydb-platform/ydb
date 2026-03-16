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

#include <geos/operation/overlayng/EdgeKey.h>
#include <geos/operation/overlayng/Edge.h>
#include <geos/geom/Dimension.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>


namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using geos::geom::Coordinate;

EdgeKey::EdgeKey(const Edge* edge)
{
    initPoints(edge);
}

/*private*/
void
EdgeKey::initPoints(const Edge* edge)
{
    bool direction = edge->direction();
    if (direction) {
        init(edge->getCoordinate(0),
             edge->getCoordinate(1));
    }
    else {
        size_t len = edge->size();
        init(edge->getCoordinate(len - 1),
             edge->getCoordinate(len - 2));
    }
}

/*private*/
void
EdgeKey::init(const Coordinate& p0, const Coordinate& p1)
{
    p0x = p0.x;
    p0y = p0.y;
    p1x = p1.x;
    p1y = p1.y;
}

/*public*/
int
EdgeKey::compareTo(const EdgeKey* ek) const
{
    if (p0x < ek->p0x) return -1;
    if (p0x > ek->p0x) return 1;
    if (p0y < ek->p0y) return -1;
    if (p0y > ek->p0y) return 1;
    // first points are equal, compare second
    if (p1x < ek->p1x) return -1;
    if (p1x > ek->p1x) return 1;
    if (p1y < ek->p1y) return -1;
    if (p1y > ek->p1y) return 1;
    return 0;
}

/*public*/
bool
EdgeKey::equals(const EdgeKey* ek) const
{
    return p0x == ek->p0x
        && p0y == ek->p0y
        && p1x == ek->p1x
        && p1y == ek->p1y;
}

bool
operator<(const EdgeKey& ek1, const EdgeKey& ek2)
{
    return ek1.compareTo(&ek2) < 0;
}

bool
operator==(const EdgeKey& ek1, const EdgeKey& ek2)
{
    return ek1.equals(&ek2);
}




} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
