/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/OffsetCurveSetBuilder.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/constants.h>
#include <geos/algorithm/Distance.h>
#include <geos/algorithm/Orientation.h>
#include <geos/algorithm/MinimumDiameter.h>
#include <geos/util/UnsupportedOperationException.h>
#include <geos/operation/buffer/OffsetCurveSetBuilder.h>
#include <geos/operation/buffer/OffsetCurveBuilder.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Point.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Location.h>
#include <geos/geom/Triangle.h>
#include <geos/geom/Position.h>
#include <geos/geomgraph/Label.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/util.h>
#include <geos/io/WKTWriter.h>

#include <algorithm> // for min
#include <cmath>
#include <cassert>
#include <iomanip>
#include <memory>
#include <vector>
#include <typeinfo>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

//using namespace geos::operation::overlay;
using namespace geos::geom;
using namespace geos::noding; // SegmentString
using namespace geos::geomgraph; // Label, Position
using namespace geos::algorithm; // Orientation

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

OffsetCurveSetBuilder::OffsetCurveSetBuilder(const Geometry& newInputGeom,
        double newDistance, OffsetCurveBuilder& newCurveBuilder):
    inputGeom(newInputGeom),
    distance(newDistance),
    curveBuilder(newCurveBuilder),
    curveList()
{
}

OffsetCurveSetBuilder::~OffsetCurveSetBuilder()
{
    for(size_t i = 0, n = curveList.size(); i < n; ++i) {
        SegmentString* ss = curveList[i];
        delete ss;
    }
    for(size_t i = 0, n = newLabels.size(); i < n; ++i) {
        delete newLabels[i];
    }
}

/* public */
std::vector<SegmentString*>&
OffsetCurveSetBuilder::getCurves()
{
    add(inputGeom);
    return curveList;
}

/*public*/
void
OffsetCurveSetBuilder::addCurves(const std::vector<CoordinateSequence*>& lineList,
                                 geom::Location leftLoc, geom::Location rightLoc)
{
    for(size_t i = 0, n = lineList.size(); i < n; ++i) {
        CoordinateSequence* coords = lineList[i];
        addCurve(coords, leftLoc, rightLoc);
    }
}

/*private*/
void
OffsetCurveSetBuilder::addCurve(CoordinateSequence* coord,
                                geom::Location leftLoc, geom::Location rightLoc)
{
#if GEOS_DEBUG
    std::cerr << __FUNCTION__ << ": coords=" << coord->toString() << std::endl;
#endif
    // don't add null curves!
    if(coord->getSize() < 2) {
#if GEOS_DEBUG
        std::cerr << " skipped (size<2)" << std::endl;
#endif
        delete coord;
        return;
    }

    // add the edge for a coordinate list which is a raw offset curve
    Label* newlabel = new Label(0, Location::BOUNDARY, leftLoc, rightLoc);

    // coord ownership transferred to SegmentString
    SegmentString* e = new NodedSegmentString(coord, newlabel);

    // SegmentString doesnt own the sequence, so we need to delete in
    // the destructor
    newLabels.push_back(newlabel);
    curveList.push_back(e);
}


/*private*/
void
OffsetCurveSetBuilder::add(const Geometry& g)
{
    if(g.isEmpty()) {
#if GEOS_DEBUG
        std::cerr << __FUNCTION__ << ": skip empty geometry" << std::endl;
#endif
        return;
    }

    const Polygon* poly = dynamic_cast<const Polygon*>(&g);
    if(poly) {
        addPolygon(poly);
        return;
    }

    const LineString* line = dynamic_cast<const LineString*>(&g);
    if(line) {
        addLineString(line);
        return;
    }

    const Point* point = dynamic_cast<const Point*>(&g);
    if(point) {
        addPoint(point);
        return;
    }

    const GeometryCollection* collection = dynamic_cast<const GeometryCollection*>(&g);
    if(collection) {
        addCollection(collection);
        return;
    }

    std::string out = typeid(g).name();
    throw util::UnsupportedOperationException("GeometryGraph::add(Geometry &): unknown geometry type: " + out);
}

/*private*/
void
OffsetCurveSetBuilder::addCollection(const GeometryCollection* gc)
{
    for(size_t i = 0, n = gc->getNumGeometries(); i < n; i++) {
        const Geometry* g = gc->getGeometryN(i);
        add(*g);
    }
}

/*private*/
void
OffsetCurveSetBuilder::addPoint(const Point* p)
{
    // a zero or negative width buffer of a point is empty
    if(distance <= 0.0) {
        return;
    }
    const CoordinateSequence* coord = p->getCoordinatesRO();
    std::vector<CoordinateSequence*> lineList;
    curveBuilder.getLineCurve(coord, distance, lineList);

    addCurves(lineList, Location::EXTERIOR, Location::INTERIOR);
}

/*private*/
void
OffsetCurveSetBuilder::addLineString(const LineString* line)
{
    if (curveBuilder.isLineOffsetEmpty(distance)) {
        return;
    }

    auto coord = operation::valid::RepeatedPointRemover::removeRepeatedPoints(line->getCoordinatesRO());

    /**
     * Rings (closed lines) are generated with a continuous curve,
     * with no end arcs. This produces better quality linework,
     * and avoids noding issues with arcs around almost-parallel end segments.
     * See JTS #523 and #518.
     *
     * Singled-sided buffers currently treat rings as if they are lines.
     */
    if (CoordinateSequence::isRing(coord.get()) && ! curveBuilder.getBufferParameters().isSingleSided()) {
        addRingBothSides(coord.get(), distance);
    }
    else {
        std::vector<CoordinateSequence*> lineList;
        curveBuilder.getLineCurve(coord.get(), distance, lineList);
        addCurves(lineList, Location::EXTERIOR, Location::INTERIOR);
    }

}


/*private*/
void
OffsetCurveSetBuilder::addPolygon(const Polygon* p)
{
    double offsetDistance = distance;

    int offsetSide = Position::LEFT;
    if(distance < 0.0) {
        offsetDistance = -distance;
        offsetSide = Position::RIGHT;
    }

    const LinearRing* shell = p->getExteriorRing();

    // optimization - don't bother computing buffer
    // if the polygon would be completely eroded
    if(distance < 0.0 && isErodedCompletely(shell, distance)) {
#if GEOS_DEBUG
        std::cerr << __FUNCTION__ << ": polygon is eroded completely " << std::endl;
#endif
        return;
    }

    auto shellCoord =
            operation::valid::RepeatedPointRemover::removeRepeatedPoints(shell->getCoordinatesRO());

    // don't attempt to buffer a polygon
    // with too few distinct vertices
    if(distance <= 0.0 && shellCoord->size() < 3) {
        return;
    }

    addRingSide(
        shellCoord.get(),
        offsetDistance,
        offsetSide,
        Location::EXTERIOR,
        Location::INTERIOR);

    for(size_t i = 0, n = p->getNumInteriorRing(); i < n; ++i) {
        const LineString* hls = p->getInteriorRingN(i);
        const LinearRing* hole = detail::down_cast<const LinearRing*>(hls);

        // optimization - don't bother computing buffer for this hole
        // if the hole would be completely covered
        if(distance > 0.0 && isErodedCompletely(hole, -distance)) {
            continue;
        }

        auto holeCoord = valid::RepeatedPointRemover::removeRepeatedPoints(hole->getCoordinatesRO());

        // Holes are topologically labelled opposite to the shell,
        // since the interior of the polygon lies on their opposite
        // side (on the left, if the hole is oriented CCW)
        addRingSide(
            holeCoord.get(),
            offsetDistance,
            Position::opposite(offsetSide),
            Location::INTERIOR,
            Location::EXTERIOR);
    }
}

/* private */
void
OffsetCurveSetBuilder::addRingBothSides(const CoordinateSequence* coord, double p_distance)
{
    addRingSide(coord, p_distance,
                Position::LEFT,
                Location::EXTERIOR, Location::INTERIOR);
    /* Add the opposite side of the ring
    */
    addRingSide(coord, p_distance,
                Position::RIGHT,
                Location::INTERIOR, Location::EXTERIOR);
}


/* private */
void
OffsetCurveSetBuilder::addRingSide(const CoordinateSequence* coord,
                                      double offsetDistance, int side, geom::Location cwLeftLoc, geom::Location cwRightLoc)
{

    // don't bother adding ring if it is "flat" and
    // will disappear in the output
    if(offsetDistance == 0.0 && coord->size() < LinearRing::MINIMUM_VALID_SIZE) {
        return;
    }

    Location leftLoc = cwLeftLoc;
    Location rightLoc = cwRightLoc;
#if GEOS_DEBUG
    std::cerr << "OffsetCurveSetBuilder::addPolygonRing: CCW: " << Orientation::isCCW(coord) << std::endl;
#endif
    if(coord->size() >= LinearRing::MINIMUM_VALID_SIZE &&
        Orientation::isCCWArea(coord))
    {
        leftLoc = cwRightLoc;
        rightLoc = cwLeftLoc;
#if GEOS_DEBUG
        std::cerr << " side " << side << " becomes " << Position::opposite(side) << std::endl;
#endif
        side = Position::opposite(side);
    }
    std::vector<CoordinateSequence*> lineList;
    curveBuilder.getRingCurve(coord, side, offsetDistance, lineList);
    // ASSERT: lineList contains exactly 1 curve (this is teh JTS semantics)
    if (lineList.size() > 0) {
        const CoordinateSequence* curve = lineList[0];
        /**
         * If the offset curve has inverted completely it will produce
         * an unwanted artifact in the result, so skip it.
         */
        if (isRingCurveInverted(coord, offsetDistance, curve)) {
            for (auto* line: lineList) {
                delete line;
            }
            return;
        }
    }
    addCurves(lineList, leftLoc, rightLoc);
}

static const int MAX_INVERTED_RING_SIZE = 9;
static const double INVERTED_CURVE_VERTEX_FACTOR = 4;
static const double NEARNESS_FACTOR = 0.99;

/* private static*/
bool
OffsetCurveSetBuilder::isRingCurveInverted(
    const CoordinateSequence* inputRing, double dist,
    const CoordinateSequence* curveRing)
{
    if (dist == 0.0) return false;
    /**
     * Only proper rings can invert.
     */
    if (inputRing->size() <= 3) return false;
    /**
     * Heuristic based on low chance that a ring with many vertices will invert.
     * This low limit ensures this test is fairly efficient.
     */
    if (inputRing->size() >= MAX_INVERTED_RING_SIZE) return false;

    /**
     * An inverted curve has no more points than the input ring.
     * This also eliminates concave inputs (which will produce fillet arcs)
     */
    if (curveRing->size() > INVERTED_CURVE_VERTEX_FACTOR * inputRing->size()) return false;

    /**
     * If curve contains points which are on the buffer, 
     * it is not inverted and can be included in the raw curves.
     */
    if (hasPointOnBuffer(inputRing, dist, curveRing))
      return false;

    //-- curve is inverted, so discard it
    return true;
}

/* private static */
bool
OffsetCurveSetBuilder::hasPointOnBuffer(
    const CoordinateSequence* inputRing, double dist, 
    const CoordinateSequence* curveRing) 
{
    double distTol = NEARNESS_FACTOR * fabs(dist);

    for (std::size_t i = 0; i < curveRing->size(); i++) {
        const Coordinate& v = curveRing->getAt(i);

        //-- check curve vertices
        double distVertex = Distance::pointToSegmentString(v, inputRing);
        if (distVertex > distTol) {
            return true; 
        }

        //-- check curve segment midpoints
        std::size_t iNext = (i < curveRing->size() - 1) ? i + 1 : 0;
        const Coordinate& vnext = curveRing->getAt(iNext);
        Coordinate midPt = LineSegment::midPoint(v, vnext);

        double distMid = Distance::pointToSegmentString(midPt, inputRing);
        if (distMid > distTol) {
            return true; 
        }
    }
    return false;
}

/*private*/
bool
OffsetCurveSetBuilder::isErodedCompletely(const LinearRing* ring,
        double bufferDistance)
{
    const CoordinateSequence* ringCoord = ring->getCoordinatesRO();

    // degenerate ring has no area
    if(ringCoord->getSize() < 4) {
        return bufferDistance < 0;
    }

    // important test to eliminate inverted triangle bug
    // also optimizes erosion test for triangles
    if(ringCoord->getSize() == 4) {
        return isTriangleErodedCompletely(ringCoord, bufferDistance);
    }

    const Envelope* env = ring->getEnvelopeInternal();
    double envMinDimension = std::min(env->getHeight(), env->getWidth());
    if(bufferDistance < 0.0 && 2 * std::abs(bufferDistance) > envMinDimension) {
        return true;
    }
    return false;
}

/*private*/
bool
OffsetCurveSetBuilder::isTriangleErodedCompletely(
    const CoordinateSequence* triangleCoord, double bufferDistance)
{
    Triangle tri(triangleCoord->getAt(0), triangleCoord->getAt(1), triangleCoord->getAt(2));

    Coordinate inCentre;
    tri.inCentre(inCentre);
    double distToCentre = Distance::pointToSegment(inCentre, tri.p0, tri.p1);
    bool ret = distToCentre < std::fabs(bufferDistance);
    return ret;
}


} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos
