/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/distance/DistanceToPoint.java 1.1 (JTS-1.9)
 *
 **********************************************************************/

#include <geos/algorithm/distance/DistanceToPoint.h>
#include <geos/algorithm/distance/PointPairDistance.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/GeometryCollection.h>

#include <typeinfo> // for dynamic_cast
#include <cassert>

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm
namespace distance { // geos.algorithm.distance

/* public static */
void
DistanceToPoint::computeDistance(const geom::Geometry& geom,
                                 const geom::Coordinate& pt,
                                 PointPairDistance& ptDist)
{
    if (geom.isEmpty()) {
        ptDist.initialize();
        return;
    }
    if(const LineString* ls = dynamic_cast<const LineString*>(&geom)) {
        computeDistance(*ls, pt, ptDist);
    }
    else if(const Polygon* pl = dynamic_cast<const Polygon*>(&geom)) {
        computeDistance(*pl, pt, ptDist);
    }
    else if(const GeometryCollection* gc = dynamic_cast<const GeometryCollection*>(&geom)) {
        for(size_t i = 0; i < gc->getNumGeometries(); i++) {
            const Geometry* g = gc->getGeometryN(i);
            computeDistance(*g, pt, ptDist);
        }
    }
    else {
        // assume geom is Point
        ptDist.setMinimum(*(geom.getCoordinate()), pt);
    }
}

/* public static */
void
DistanceToPoint::computeDistance(const geom::LineString& line,
                                 const geom::Coordinate& pt,
                                 PointPairDistance& ptDist)
{
    const CoordinateSequence* coordsRO = line.getCoordinatesRO();
    const CoordinateSequence& coords = *coordsRO;

    size_t npts = coords.size();
    if(! npts) {
        return;    // can this ever be ?
    }

    LineSegment tempSegment;
    Coordinate closestPt;

    Coordinate* segPts[2] = { &(tempSegment.p0), &(tempSegment.p1) };
    tempSegment.p0 = coords.getAt(0);
    for(size_t i = 1; i < npts; ++i) {
        *(segPts[i % 2]) = coords.getAt(i);

        // this is somewhat inefficient - could do better
        tempSegment.closestPoint(pt, closestPt);
        ptDist.setMinimum(closestPt, pt);
    }
}

/* public static */
void
DistanceToPoint::computeDistance(const geom::LineSegment& segment,
                                 const geom::Coordinate& pt,
                                 PointPairDistance& ptDist)
{
    Coordinate closestPt;
    segment.closestPoint(pt, closestPt);
    ptDist.setMinimum(closestPt, pt);
}

/* public static */
void
DistanceToPoint::computeDistance(const geom::Polygon& poly,
                                 const geom::Coordinate& pt,
                                 PointPairDistance& ptDist)
{
    computeDistance(*(poly.getExteriorRing()), pt, ptDist);
    for(size_t i = 0, n = poly.getNumInteriorRing(); i < n; ++i) {
        computeDistance(*(poly.getInteriorRingN(i)), pt, ptDist);
    }
}


} // namespace geos.algorithm.distance
} // namespace geos.algorithm
} // namespace geos

