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
 * Last port: algorithm/MinimumDiameter.java r966
 *
 **********************************************************************/

#include <geos/constants.h>
#include <geos/algorithm/MinimumDiameter.h>
#include <geos/algorithm/ConvexHull.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>

#include <typeinfo>
#include <cmath> // for fabs()

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm

/**
 * Computes the minimum diameter of a Geometry.
 * The minimum diameter is defined to be the
 * width of the smallest band that
 * contains the geometry,
 * where a band is a strip of the plane defined
 * by two parallel lines.
 * This can be thought of as the smallest hole that the geometry can be
 * moved through, with a single rotation.
 *
 * The first step in the algorithm is computing the convex hull of the Geometry.
 * If the input Geometry is known to be convex, a hint can be supplied to
 * avoid this computation.
 *
 * @see ConvexHull
 *
 * @version 1.4
 */

/**
 * Compute a minimum diameter for a giver {@link Geometry}.
 *
 * @param geom a Geometry
 */
MinimumDiameter::MinimumDiameter(const Geometry* newInputGeom)
{
    minWidthPt = Coordinate::getNull();
    minPtIndex = 0;
    minWidth = 0.0;
    inputGeom = newInputGeom;
    isConvex = false;
    convexHullPts = nullptr;
}

/**
 * Compute a minimum diameter for a giver Geometry,
 * with a hint if
 * the Geometry is convex
 * (e.g. a convex Polygon or LinearRing,
 * or a two-point LineString, or a Point).
 *
 * @param geom a Geometry which is convex
 * @param isConvex <code>true</code> if the input geometry is convex
 */
MinimumDiameter::MinimumDiameter(const Geometry* newInputGeom, const bool newIsConvex)
{
    minWidthPt = Coordinate::getNull();
    minWidth = 0.0;
    inputGeom = newInputGeom;
    isConvex = newIsConvex;
    convexHullPts = nullptr;
}

/**
 * Gets the length of the minimum diameter of the input Geometry
 *
 * @return the length of the minimum diameter
 */
double
MinimumDiameter::getLength()
{
    computeMinimumDiameter();
    return minWidth;
}

/**
 * Gets the {@link Coordinate} forming one end of the minimum diameter
 *
 * @return a coordinate forming one end of the minimum diameter
 */
const Coordinate&
MinimumDiameter::getWidthCoordinate()
{
    computeMinimumDiameter();
    return minWidthPt;
}

/**
 * Gets the segment forming the base of the minimum diameter
 *
 * @return the segment forming the base of the minimum diameter
 */
std::unique_ptr<LineString>
MinimumDiameter::getSupportingSegment()
{
    computeMinimumDiameter();
    const GeometryFactory* fact = inputGeom->getFactory();
    return minBaseSeg.toGeometry(*fact);
}

/**
 * Gets a LineString which is a minimum diameter
 *
 * @return a LineString which is a minimum diameter
 */
std::unique_ptr<LineString>
MinimumDiameter::getDiameter()
{
    computeMinimumDiameter();
    // return empty linestring if no minimum width calculated
    if(minWidthPt.isNull()) {
        return std::unique_ptr<LineString>(inputGeom->getFactory()->createLineString(nullptr));
    }

    Coordinate basePt;
    minBaseSeg.project(minWidthPt, basePt);

    auto cl = inputGeom->getFactory()->getCoordinateSequenceFactory()->create(2);
    cl->setAt(basePt, 0);
    cl->setAt(minWidthPt, 1);
    return inputGeom->getFactory()->createLineString(std::move(cl));
}

/* private */
void
MinimumDiameter::computeMinimumDiameter()
{
    // check if computation is cached
    if(!minWidthPt.isNull()) {
        return;
    }
    if(isConvex) {
        computeWidthConvex(inputGeom);
    }
    else {
        ConvexHull ch(inputGeom);
        std::unique_ptr<Geometry> convexGeom = ch.getConvexHull();
        computeWidthConvex(convexGeom.get());
    }
}

/* private */
void
MinimumDiameter::computeWidthConvex(const Geometry* geom)
{
    //System.out.println("Input = " + geom);
    if(typeid(*geom) == typeid(Polygon)) {
        const Polygon* p = dynamic_cast<const Polygon*>(geom);
        convexHullPts = p->getExteriorRing()->getCoordinates();
    }
    else {
        convexHullPts = geom->getCoordinates();
    }

    // special cases for lines or points or degenerate rings
    switch(convexHullPts->getSize()) {
    case 0:
        minWidth = 0.0;
        minWidthPt = Coordinate::getNull();
        break;
    case 1:
        minWidth = 0.0;
        minWidthPt = convexHullPts->getAt(0);
        minBaseSeg.p0 = convexHullPts->getAt(0);
        minBaseSeg.p1 = convexHullPts->getAt(0);
        break;
    case 2:
    case 3:
        minWidth = 0.0;
        minWidthPt = convexHullPts->getAt(0);
        minBaseSeg.p0 = convexHullPts->getAt(0);
        minBaseSeg.p1 = convexHullPts->getAt(1);
        break;
    default:
        computeConvexRingMinDiameter(convexHullPts.get());
    }
}

/**
 * Compute the width information for a ring of {@link Coordinate}s.
 * Leaves the width information in the instance variables.
 *
 * @param pts
 * @return
 */
void
MinimumDiameter::computeConvexRingMinDiameter(const CoordinateSequence* pts)
{
    minWidth = DoubleInfinity;
    unsigned int currMaxIndex = 1;
    LineSegment seg;

    // compute the max distance for all segments in the ring, and pick the minimum
    const std::size_t npts = pts->getSize();
    for(std::size_t i = 1; i < npts; ++i) {
        seg.p0 = pts->getAt(i - 1);
        seg.p1 = pts->getAt(i);
        currMaxIndex = findMaxPerpDistance(pts, &seg, currMaxIndex);
    }
}

unsigned int
MinimumDiameter::findMaxPerpDistance(const CoordinateSequence* pts,
                                     const LineSegment* seg, unsigned int startIndex)
{
    double maxPerpDistance = seg->distancePerpendicular(pts->getAt(startIndex));
    double nextPerpDistance = maxPerpDistance;
    unsigned int maxIndex = startIndex;
    unsigned int nextIndex = maxIndex;
    while(nextPerpDistance >= maxPerpDistance) {
        maxPerpDistance = nextPerpDistance;
        maxIndex = nextIndex;
        nextIndex = getNextIndex(pts, maxIndex);
        if (nextIndex == startIndex)
            break;
        nextPerpDistance = seg->distancePerpendicular(pts->getAt(nextIndex));
    }

    // found maximum width for this segment - update global min dist if appropriate
    if(maxPerpDistance < minWidth) {
        minPtIndex = maxIndex;
        minWidth = maxPerpDistance;
        minWidthPt = pts->getAt(minPtIndex);
        minBaseSeg = *seg;
    }
    return maxIndex;
}

unsigned int
MinimumDiameter::getNextIndex(const CoordinateSequence* pts,
                              unsigned int index)
{
    if(++index >= pts->getSize()) {
        index = 0;
    }
    return index;
}

std::unique_ptr<Geometry>
MinimumDiameter::getMinimumRectangle()
{
    computeMinimumDiameter();

    if(minWidthPt.isNull() || !convexHullPts) {
        //return empty polygon
        return std::unique_ptr<Geometry>(inputGeom->getFactory()->createPolygon());
    }

    // check if minimum rectangle is degenerate (a point or line segment)
    if(minWidth == 0.0) {
        if(minBaseSeg.p0.equals2D(minBaseSeg.p1)) {
            return std::unique_ptr<Geometry>(inputGeom->getFactory()->createPoint(minBaseSeg.p0));
        }
      //-- Min rectangle is a line. Use the diagonal of the extent
      return computeMaximumLine(convexHullPts.get(), inputGeom->getFactory());
    }

    // deltas for the base segment of the minimum diameter
    double dx = minBaseSeg.p1.x - minBaseSeg.p0.x;
    double dy = minBaseSeg.p1.y - minBaseSeg.p0.y;

    double minPara = DoubleInfinity;
    double maxPara = DoubleNegInfinity;
    double minPerp = DoubleInfinity;
    double maxPerp = DoubleNegInfinity;

    // compute maxima and minima of lines parallel and perpendicular to base segment
    std::size_t const n = convexHullPts->getSize();
    for(std::size_t i = 0; i < n; ++i) {

        double paraC = computeC(dx, dy, convexHullPts->getAt(i));
        if(paraC > maxPara) {
            maxPara = paraC;
        }
        if(paraC < minPara) {
            minPara = paraC;
        }

        double perpC = computeC(-dy, dx, convexHullPts->getAt(i));
        if(perpC > maxPerp) {
            maxPerp = perpC;
        }
        if(perpC < minPerp) {
            minPerp = perpC;
        }
    }

    // compute lines along edges of minimum rectangle
    LineSegment maxPerpLine = computeSegmentForLine(-dx, -dy, maxPerp);
    LineSegment minPerpLine = computeSegmentForLine(-dx, -dy, minPerp);
    LineSegment maxParaLine = computeSegmentForLine(-dy, dx, maxPara);
    LineSegment minParaLine = computeSegmentForLine(-dy, dx, minPara);

    // compute vertices of rectangle (where the para/perp max & min lines intersect)
    Coordinate p0 = maxParaLine.lineIntersection(maxPerpLine);
    Coordinate p1 = minParaLine.lineIntersection(maxPerpLine);
    Coordinate p2 = minParaLine.lineIntersection(minPerpLine);
    Coordinate p3 = maxParaLine.lineIntersection(minPerpLine);

    const CoordinateSequenceFactory* csf =
        inputGeom->getFactory()->getCoordinateSequenceFactory();

    auto seq = csf->create(5, 2);
    seq->setAt(p0, 0);
    seq->setAt(p1, 1);
    seq->setAt(p2, 2);
    seq->setAt(p3, 3);
    seq->setAt(p0, 4); // close

    std::unique_ptr<LinearRing> shell = inputGeom->getFactory()->createLinearRing(std::move(seq));
    return inputGeom->getFactory()->createPolygon(std::move(shell));
}

// private static
std::unique_ptr<Geometry>
MinimumDiameter::computeMaximumLine(const geom::CoordinateSequence* pts,
        const GeometryFactory* factory)
{
    //-- find max and min pts for X and Y
    Coordinate ptMinX = pts->getAt(0);
    Coordinate ptMaxX = pts->getAt(0);
    Coordinate ptMinY = pts->getAt(0);
    Coordinate ptMaxY = pts->getAt(0);

    std::size_t const n = pts->getSize();
    for(std::size_t i = 1; i < n; ++i) {
      const Coordinate& p = pts->getAt(i);
      if (p.x < ptMinX.x) ptMinX = p;
      if (p.x > ptMaxX.x) ptMaxX = p;
      if (p.y < ptMinY.y) ptMinY = p;
      if (p.y > ptMaxY.y) ptMaxY = p;
    }
    Coordinate p0 = ptMinX;
    Coordinate p1 = ptMaxX;
    //-- line is vertical - use Y pts
    if (p0.x == p1.x) {
      p0 = ptMinY;
      p1 = ptMaxY;
    }

    const CoordinateSequenceFactory* csf =
        factory->getCoordinateSequenceFactory();
    auto seq = csf->create(2, 2);
    seq->setAt(p0, 0);
    seq->setAt(p1, 1);

    return factory->createLineString(std::move(seq));
}

double
MinimumDiameter::computeC(double a, double b, const Coordinate& p)
{
    return a * p.y - b * p.x;
}

LineSegment
MinimumDiameter::computeSegmentForLine(double a, double b, double c)
{
    Coordinate p0;
    Coordinate p1;
    /*
    * Line eqn is ax + by = c
    * Slope is a/b.
    * If slope is steep, use y values as the inputs
    */
    if(fabs(b) > fabs(a)) {
        p0 = Coordinate(0.0, c / b);
        p1 = Coordinate(1.0, c / b - a / b);
    }
    else {
        p0 = Coordinate(c / a, 0.0);
        p1 = Coordinate(c / a - b / a, 1.0);
    }
    return LineSegment(p0, p1);
}


std::unique_ptr<Geometry>
MinimumDiameter::getMinimumRectangle(Geometry* geom)
{
    MinimumDiameter md(geom);
    return md.getMinimumRectangle();
}

std::unique_ptr<Geometry>
MinimumDiameter::getMinimumDiameter(Geometry* geom)
{
    MinimumDiameter md(geom);
    return md.getDiameter();
}

} // namespace geos.algorithm
} // namespace geos
