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
 * Last port: algorithm/InteriorPointLine.java r317 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/constants.h>
#include <geos/algorithm/InteriorPointLine.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequence.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm

InteriorPointLine::InteriorPointLine(const Geometry* g)
{
    minDistance = DoubleInfinity;
    hasInterior = false;
    if (g->getCentroid(centroid)) {
#if GEOS_DEBUG
        std::cerr << "Centroid: " << centroid << std::endl;
#endif
        addInterior(g);
    }
    if (!hasInterior) {
        addEndpoints(g);
    }
}

/* private
 *
 * Tests the interior vertices (if any)
 * defined by a linear Geometry for the best inside point.
 * If a Geometry is not of dimension 1 it is not tested.
 * @param geom the geometry to add
 */
void
InteriorPointLine::addInterior(const Geometry* geom)
{
    const LineString* ls = dynamic_cast<const LineString*>(geom);
    if (ls) {
        addInterior(ls->getCoordinatesRO());
        return;
    }

    const GeometryCollection* gc = dynamic_cast<const GeometryCollection*>(geom);
    if (gc) {
        for (std::size_t i = 0, n = gc->getNumGeometries(); i < n; i++) {
            addInterior(gc->getGeometryN(i));
        }
    }
}

void
InteriorPointLine::addInterior(const CoordinateSequence* pts)
{
    const std::size_t n = pts->getSize() - 1;
    for (std::size_t i = 1; i < n; ++i) {
        add(pts->getAt(i));
    }
}

/* private
 *
 * Tests the endpoint vertices
 * defined by a linear Geometry for the best inside point.
 * If a Geometry is not of dimension 1 it is not tested.
 * @param geom the geometry to add
 */
void
InteriorPointLine::addEndpoints(const Geometry* geom)
{
    const LineString* ls = dynamic_cast<const LineString*>(geom);
    if (ls) {
        addEndpoints(ls->getCoordinatesRO());
        return;
    }

    const GeometryCollection* gc = dynamic_cast<const GeometryCollection*>(geom);
    if (gc) {
        for (std::size_t i = 0, n = gc->getNumGeometries(); i < n; i++) {
            addEndpoints(gc->getGeometryN(i));
        }
    }
}

void
InteriorPointLine::addEndpoints(const CoordinateSequence* pts)
{
    size_t npts = pts->size();
    if (npts) {
        add(pts->getAt(0));
        if (npts > 1) {
            add(pts->getAt(npts - 1));
        }
    }
}

/*private*/
void
InteriorPointLine::add(const Coordinate& point)
{

    double dist = point.distance(centroid);
#if GEOS_DEBUG
    std::cerr << "point " << point << " dist " << dist << ", minDistance " << minDistance << std::endl;
#endif
    if (!hasInterior || dist < minDistance) {
        interiorPoint = point;
#if GEOS_DEBUG
        std::cerr << " is new InteriorPoint" << std::endl;
#endif
        minDistance = dist;
        hasInterior = true;
    }
}

bool
InteriorPointLine::getInteriorPoint(Coordinate& ret) const
{
    if (! hasInterior) {
        return false;
    }
    ret = interiorPoint;
    return true;
}

} // namespace geos.algorithm
} // namespace geos

