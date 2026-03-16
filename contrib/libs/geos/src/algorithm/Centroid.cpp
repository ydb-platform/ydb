/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2013 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/Centroid.java r728 (JTS-0.13+)
 *
 **********************************************************************/

#include <geos/algorithm/Centroid.h>
#include <geos/algorithm/Orientation.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/LineString.h>


#include <cmath> // for std::abs

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm

/* static public */
bool
Centroid::getCentroid(const Geometry& geom, Coordinate& pt)
{
    Centroid cent(geom);
    return cent.getCentroid(pt);
}

/* public */
bool
Centroid::getCentroid(Coordinate& cent) const
{
    if(std::abs(areasum2) > 0.0) {
        cent.x = cg3.x / 3 / areasum2;
        cent.y = cg3.y / 3 / areasum2;
    }
    else if(totalLength > 0.0) {
        // if polygon was degenerate, compute linear centroid instead
        cent.x = lineCentSum.x / totalLength;
        cent.y = lineCentSum.y / totalLength;
    }
    else if(ptCount > 0) {
        cent.x = ptCentSum.x / ptCount;
        cent.y = ptCentSum.y / ptCount;
    }
    else {
        return false;
    }
    return true;
}

/* private */
void
Centroid::add(const Geometry& geom)
{
    if(geom.isEmpty()) {
        return;
    }

    if(const Point* pt = dynamic_cast<const Point*>(&geom)) {
        addPoint(*pt->getCoordinate());
    }
    else if(const LineString* ls = dynamic_cast<const LineString*>(&geom)) {
        addLineSegments(*ls->getCoordinatesRO());
    }
    else if(const Polygon* p = dynamic_cast<const Polygon*>(&geom)) {
        add(*p);
    }
    else if(const GeometryCollection* g = dynamic_cast<const GeometryCollection*>(&geom)) {
        for(size_t i = 0; i < g->getNumGeometries(); i++) {
            add(*g->getGeometryN(i));
        }
    }
}

/* private */
void
Centroid::setAreaBasePoint(const Coordinate& basePt)
{
    areaBasePt.reset(new Coordinate(basePt));
}

/* private */
void
Centroid::add(const Polygon& poly)
{
    addShell(*poly.getExteriorRing()->getCoordinatesRO());
    for(size_t i = 0; i < poly.getNumInteriorRing(); i++) {
        addHole(*poly.getInteriorRingN(i)->getCoordinatesRO());
    }
}

/* private */
void
Centroid::addShell(const CoordinateSequence& pts)
{
    size_t len = pts.size();
    if(len > 0) {
        setAreaBasePoint(pts[0]);
    }
    bool isPositiveArea = ! Orientation::isCCW(&pts);
    for(size_t i = 0; i < len - 1; ++i) {
        addTriangle(*areaBasePt, pts[i], pts[i + 1], isPositiveArea);
    }
    addLineSegments(pts);
}

/* private */
void
Centroid::addHole(const CoordinateSequence& pts)
{
    bool isPositiveArea = Orientation::isCCW(&pts);
    for(size_t i = 0, e = pts.size() - 1; i < e; ++i) {
        addTriangle(*areaBasePt, pts[i], pts[i + 1], isPositiveArea);
    }
    addLineSegments(pts);
}

/* private */
void
Centroid::addTriangle(const Coordinate& p0, const Coordinate& p1, const Coordinate& p2, bool isPositiveArea)
{
    double sign = (isPositiveArea) ? 1.0 : -1.0;
    centroid3(p0, p1, p2, triangleCent3);
    double a2 = area2(p0, p1, p2);
    cg3.x += sign * a2 * triangleCent3.x;
    cg3.y += sign * a2 * triangleCent3.y;
    areasum2 += sign * a2;
}

/* static private */
void
Centroid::centroid3(const Coordinate& p1, const Coordinate& p2, const Coordinate& p3, Coordinate& c)
{
    c.x = p1.x + p2.x + p3.x;
    c.y = p1.y + p2.y + p3.y;
    return;
}

/* static private */
double
Centroid::area2(const Coordinate& p1, const Coordinate& p2, const Coordinate& p3)
{
    return
        (p2.x - p1.x) * (p3.y - p1.y) -
        (p3.x - p1.x) * (p2.y - p1.y);
}

/* private */
void
Centroid::addLineSegments(const CoordinateSequence& pts)
{
    size_t npts = pts.size();
    double lineLen = 0.0;
    for(size_t i = 0; i < npts - 1; i++) {
        double segmentLen = pts[i].distance(pts[i + 1]);
        if(segmentLen == 0.0) {
            continue;
        }

        lineLen += segmentLen;

        double midx = (pts[i].x + pts[i + 1].x) / 2;
        lineCentSum.x += segmentLen * midx;
        double midy = (pts[i].y + pts[i + 1].y) / 2;
        lineCentSum.y += segmentLen * midy;
    }
    totalLength += lineLen;
    if(lineLen == 0.0 && npts > 0) {
        addPoint(pts[0]);
    }
}

/* private */
void
Centroid::addPoint(const Coordinate& pt)
{
    ptCount += 1;
    ptCentSum.x += pt.x;
    ptCentSum.y += pt.y;
}

} // namespace geos.algorithm
} // namespace geos
