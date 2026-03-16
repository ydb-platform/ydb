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

#include <geos/constants.h>
#include <geos/algorithm/InteriorPointPoint.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>
#include <geos/geom/CoordinateSequence.h>

#include <typeinfo>
#include <cassert>

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm

/*public*/
InteriorPointPoint::InteriorPointPoint(const Geometry* g)
{
    minDistance = DoubleInfinity;
    if (! g->getCentroid(centroid)) {
        hasInterior = false;
    }
    else {
        hasInterior = true;
        add(g);
    }
}

/*private*/
void
InteriorPointPoint::add(const Geometry* geom)
{
    const Point* po = dynamic_cast<const Point*>(geom);
    if (po) {
        add(po->getCoordinate());
        return;
    }

    const GeometryCollection* gc = dynamic_cast<const GeometryCollection*>(geom);
    if (gc) {
        for (std::size_t i = 0, n = gc->getNumGeometries(); i < n; i++) {
            add(gc->getGeometryN(i));
        }
    }
}

/*private*/
void
InteriorPointPoint::add(const Coordinate* point)
{
    assert(point);    // we wouldn't been called if this was an empty geom
    double dist = point->distance(centroid);
    if (dist < minDistance) {
        interiorPoint = *point;
        minDistance = dist;
    }
}

/*public*/
bool
InteriorPointPoint::getInteriorPoint(Coordinate& ret) const
{
    if (! hasInterior) {
        return false;
    }
    ret = interiorPoint;
    return true;
}

} // namespace geos.algorithm
} // namespace geos

