/**********************************************************************
 *
 * GEOS-Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/OffsetSegmentGenerator.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <cassert>
#include <cmath>
#include <vector>

#include <geos/algorithm/Orientation.h>
#include <geos/algorithm/Angle.h>
#include <geos/operation/buffer/OffsetSegmentGenerator.h>
#include <geos/operation/buffer/OffsetSegmentString.h>
#include <geos/operation/buffer/BufferOp.h>
#include <geos/operation/buffer/BufferInputLineSimplifier.h>
#include <geos/operation/buffer/BufferParameters.h>
#include <geos/geom/Position.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/algorithm/NotRepresentableException.h>
#include <geos/algorithm/Intersection.h>
#include <geos/util.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;
using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/*private data*/
const double OffsetSegmentGenerator::CURVE_VERTEX_SNAP_DISTANCE_FACTOR = 1.0E-6;
const double OffsetSegmentGenerator::PI = 3.14159265358979;
const double OffsetSegmentGenerator::OFFSET_SEGMENT_SEPARATION_FACTOR = 1.0E-3;
const double OffsetSegmentGenerator::INSIDE_TURN_VERTEX_SNAP_DISTANCE_FACTOR = 1.0E-3;
const double OffsetSegmentGenerator::SIMPLIFY_FACTOR = 100.0;

/*public*/
OffsetSegmentGenerator::OffsetSegmentGenerator(
    const PrecisionModel* newPrecisionModel,
    const BufferParameters& nBufParams,
    double dist)
    :
    maxCurveSegmentError(0.0),
    closingSegLengthFactor(1),
    segList(),
    distance(dist),
    precisionModel(newPrecisionModel),
    bufParams(nBufParams),
    li(),
    s0(),
    s1(),
    s2(),
    seg0(),
    seg1(),
    offset0(),
    offset1(),
    side(0),
    _hasNarrowConcaveAngle(false),
    endCapIndex(0)
{
    // compute intersections in full precision, to provide accuracy
    // the points are rounded as they are inserted into the curve line
    filletAngleQuantum = PI / 2.0 / bufParams.getQuadrantSegments();

    /*
     * Non-round joins cause issues with short closing segments,
     * so don't use them.  In any case, non-round joins
     * only really make sense for relatively small buffer distances.
     */
    if(bufParams.getQuadrantSegments() >= 8
            && bufParams.getJoinStyle() == BufferParameters::JOIN_ROUND) {
        closingSegLengthFactor = MAX_CLOSING_SEG_LEN_FACTOR;
    }

    init(distance);
}

/*private*/
void
OffsetSegmentGenerator::init(double newDistance)
{
    distance = newDistance;
    maxCurveSegmentError = distance * (1 - cos(filletAngleQuantum / 2.0));

    // Point list needs to be reset
    segList.reset();
    segList.setPrecisionModel(precisionModel);

    /*
     * Choose the min vertex separation as a small fraction of
     * the offset distance.
     */
    segList.setMinimumVertexDistance(
        distance * CURVE_VERTEX_SNAP_DISTANCE_FACTOR);
}

/*public*/
void
OffsetSegmentGenerator::initSideSegments(const Coordinate& nS1,
        const Coordinate& nS2, int nSide)
{
    s1 = nS1;
    s2 = nS2;
    side = nSide;
    seg1.setCoordinates(s1, s2);
    computeOffsetSegment(seg1, side, distance, offset1);
}

/*public*/
void
OffsetSegmentGenerator::addNextSegment(const Coordinate& p, bool addStartPoint)
{

    // do nothing if points are equal
    if(s2 == p) {
        return;
    }

    // s0-s1-s2 are the coordinates of the previous segment
    // and the current one
    s0 = s1;
    s1 = s2;
    s2 = p;
    seg0.setCoordinates(s0, s1);
    computeOffsetSegment(seg0, side, distance, offset0);
    seg1.setCoordinates(s1, s2);
    computeOffsetSegment(seg1, side, distance, offset1);

    int orientation = Orientation::index(s0, s1, s2);
    bool outsideTurn =
        (orientation == Orientation::CLOCKWISE
         && side == Position::LEFT)
        ||
        (orientation == Orientation::COUNTERCLOCKWISE
         && side == Position::RIGHT);

    if(orientation == 0) {
        // lines are collinear
        addCollinear(addStartPoint);
    }
    else if(outsideTurn) {
        addOutsideTurn(orientation, addStartPoint);
    }
    else {
        // inside turn
        addInsideTurn(orientation, addStartPoint);
    }
}

/*private*/
void
OffsetSegmentGenerator::computeOffsetSegment(const LineSegment& seg, int p_side,
        double p_distance, LineSegment& offset)
{
    int sideSign = p_side == Position::LEFT ? 1 : -1;
    double dx = seg.p1.x - seg.p0.x;
    double dy = seg.p1.y - seg.p0.y;
    double len = sqrt(dx * dx + dy * dy);
    // u is the vector that is the length of the offset,
    // in the direction of the segment
    double ux = sideSign * p_distance * dx / len;
    double uy = sideSign * p_distance * dy / len;
    offset.p0.x = seg.p0.x - uy;
    offset.p0.y = seg.p0.y + ux;
    offset.p1.x = seg.p1.x - uy;
    offset.p1.y = seg.p1.y + ux;
}

/*public*/
void
OffsetSegmentGenerator::addLineEndCap(const Coordinate& p0, const Coordinate& p1)
{
    LineSegment seg(p0, p1);

    LineSegment offsetL;
    computeOffsetSegment(seg, Position::LEFT, distance, offsetL);
    LineSegment offsetR;
    computeOffsetSegment(seg, Position::RIGHT, distance, offsetR);

    double dx = p1.x - p0.x;
    double dy = p1.y - p0.y;
    double angle = atan2(dy, dx);

    switch(bufParams.getEndCapStyle()) {
    case BufferParameters::CAP_ROUND:
        // add offset seg points with a fillet between them
        segList.addPt(offsetL.p1);
        addDirectedFillet(p1, angle + PI / 2.0, angle - PI / 2.0,
                  Orientation::CLOCKWISE, distance);
        segList.addPt(offsetR.p1);
        break;
    case BufferParameters::CAP_FLAT:
        // only offset segment points are added
        segList.addPt(offsetL.p1);
        segList.addPt(offsetR.p1);
        break;
    case BufferParameters::CAP_SQUARE:
        // add a square defined by extensions of the offset
        // segment endpoints
        Coordinate squareCapSideOffset;
        squareCapSideOffset.x = fabs(distance) * cos(angle);
        squareCapSideOffset.y = fabs(distance) * sin(angle);

        Coordinate squareCapLOffset(
            offsetL.p1.x + squareCapSideOffset.x,
            offsetL.p1.y + squareCapSideOffset.y);
        Coordinate squareCapROffset(
            offsetR.p1.x + squareCapSideOffset.x,
            offsetR.p1.y + squareCapSideOffset.y);
        segList.addPt(squareCapLOffset);
        segList.addPt(squareCapROffset);
        break;
    }
}

/*private*/
void
OffsetSegmentGenerator::addDirectedFillet(const Coordinate& p, const Coordinate& p0,
                                  const Coordinate& p1, int direction, double radius)
{
    double dx0 = p0.x - p.x;
    double dy0 = p0.y - p.y;
    double startAngle = atan2(dy0, dx0);
    double dx1 = p1.x - p.x;
    double dy1 = p1.y - p.y;
    double endAngle = atan2(dy1, dx1);

    if(direction == Orientation::CLOCKWISE) {
        if(startAngle <= endAngle) {
            startAngle += 2.0 * PI;
        }
    }
    else {    // direction==COUNTERCLOCKWISE
        if(startAngle >= endAngle) {
            startAngle -= 2.0 * PI;
        }
    }

    segList.addPt(p0);
    addDirectedFillet(p, startAngle, endAngle, direction, radius);
    segList.addPt(p1);
}

/*private*/
void
OffsetSegmentGenerator::addDirectedFillet(const Coordinate& p, double startAngle,
                                          double endAngle, int direction, double radius)
{
    int directionFactor = direction == Orientation::CLOCKWISE ? -1 : 1;

    double totalAngle = fabs(startAngle - endAngle);
    int nSegs = (int)(totalAngle / filletAngleQuantum + 0.5);

    // no segments because angle is less than increment-nothing to do!
    if(nSegs < 1) return;

    // double initAngle, currAngleInc;
    double angleInc = totalAngle / nSegs;
    Coordinate pt;
    for (int i = 0; i < nSegs; i++) {
        double angle = startAngle + directionFactor * i * angleInc;
        pt.x = p.x + radius * cos(angle);
        pt.y = p.y + radius * sin(angle);
        segList.addPt(pt);
    }
}

/*private*/
void
OffsetSegmentGenerator::createCircle(const Coordinate& p, double p_distance)
{
    // add start point
    Coordinate pt(p.x + p_distance, p.y);
    segList.addPt(pt);
    addDirectedFillet(p, 0.0, 2.0 * PI, -1, p_distance);
    segList.closeRing();
}

/*private*/
void
OffsetSegmentGenerator::createSquare(const Coordinate& p, double p_distance)
{
    segList.addPt(Coordinate(p.x + p_distance, p.y + p_distance));
    segList.addPt(Coordinate(p.x + p_distance, p.y - p_distance));
    segList.addPt(Coordinate(p.x - p_distance, p.y - p_distance));
    segList.addPt(Coordinate(p.x - p_distance, p.y + p_distance));
    segList.closeRing();
}

/* private */
void
OffsetSegmentGenerator::addCollinear(bool addStartPoint)
{
    /*
     * This test could probably be done more efficiently,
     * but the situation of exact collinearity should be fairly rare.
     */

    li.computeIntersection(s0, s1, s1, s2);
    auto numInt = li.getIntersectionNum();

    /*
     * if numInt is<2, the lines are parallel and in the same direction.
     * In this case the point can be ignored, since the offset lines
     * will also be parallel.
     */
    if(numInt >= 2) {
        /*
         * Segments are collinear but reversing.
         * Add an "end-cap" fillet
         * all the way around to other direction
         *
         * This case should ONLY happen for LineStrings,
         * so the orientation is always CW (Polygons can never
         * have two consecutive segments which are parallel but
         * reversed, because that would be a self intersection).
         */
        if(bufParams.getJoinStyle() == BufferParameters::JOIN_BEVEL
                || bufParams.getJoinStyle() == BufferParameters::JOIN_MITRE) {
            if(addStartPoint) {
                segList.addPt(offset0.p1);
            }
            segList.addPt(offset1.p0);
        }
        else {
            addDirectedFillet(s1, offset0.p1, offset1.p0,
                      Orientation::CLOCKWISE, distance);
        }
    }
}

/* private */
void
OffsetSegmentGenerator::addOutsideTurn(int orientation, bool addStartPoint)
{
    /*
     * Heuristic: If offset endpoints are very close together,
     * just use one of them as the corner vertex.
     * This avoids problems with computing mitre corners in the case
     * where the two segments are almost parallel
     * (which is hard to compute a robust intersection for).
     */

    if(offset0.p1.distance(offset1.p0) <
            distance * OFFSET_SEGMENT_SEPARATION_FACTOR) {
        segList.addPt(offset0.p1);
        return;
    }

    if(bufParams.getJoinStyle() == BufferParameters::JOIN_MITRE) {
        addMitreJoin(s1, offset0, offset1, distance);
    }
    else if(bufParams.getJoinStyle() == BufferParameters::JOIN_BEVEL) {
        addBevelJoin(offset0, offset1);
    }
    else {
        // add a circular fillet connecting the endpoints
        // of the offset segments
        if(addStartPoint) {
            segList.addPt(offset0.p1);
        }

        // TESTING - comment out to produce beveled joins
        addDirectedFillet(s1, offset0.p1, offset1.p0, orientation, distance);
        segList.addPt(offset1.p0);
    }
}

/* private */
void
OffsetSegmentGenerator::addInsideTurn(int orientation, bool addStartPoint)
{
    ::geos::ignore_unused_variable_warning(orientation);
    ::geos::ignore_unused_variable_warning(addStartPoint);

    // add intersection point of offset segments (if any)
    li.computeIntersection(offset0.p0, offset0.p1, offset1.p0, offset1.p1);
    if(li.hasIntersection()) {
        segList.addPt(li.getIntersection(0));
        return;
    }

    // If no intersection is detected, it means the angle is so small
    // and/or the offset so large that the offsets segments don't
    // intersect. In this case we must add a "closing segment" to make
    // sure the buffer curve is continuous,
    // fairly smooth (e.g. no sharp reversals in direction)
    // and tracks the buffer correctly around the corner.
    // The curve connects the endpoints of the segment offsets to points
    // which lie toward the centre point of the corner.
    // The joining curve will not appear in the final buffer outline,
    // since it is completely internal to the buffer polygon.
    //
    // In complex buffer cases the closing segment may cut across many
    // other segments in the generated offset curve.
    // In order to improve the performance of the noding, the closing
    // segment should be kept as short as possible.
    // (But not too short, since that would defeat it's purpose).
    // This is the purpose of the closingSegLengthFactor heuristic value.

    /*
     * The intersection test above is vulnerable to robustness errors;
     * i.e. it may be that the offsets should intersect very close to
     * their endpoints, but aren't reported as such due to rounding.
     * To handle this situation appropriately, we use the following test:
     * If the offset points are very close, don't add closing segments
     * but simply use one of the offset points
     */

    if(offset0.p1.distance(offset1.p0) <
            distance * INSIDE_TURN_VERTEX_SNAP_DISTANCE_FACTOR) {
        segList.addPt(offset0.p1);
    }
    else {
        // add endpoint of this segment offset
        segList.addPt(offset0.p1);

        // Add "closing segment" of required length.
        if(closingSegLengthFactor > 0) {
            Coordinate mid0(
                (closingSegLengthFactor * offset0.p1.x + s1.x) / (closingSegLengthFactor + 1),
                (closingSegLengthFactor * offset0.p1.y + s1.y) / (closingSegLengthFactor + 1)
            );
            segList.addPt(mid0);

            Coordinate mid1(
                (closingSegLengthFactor * offset1.p0.x + s1.x) / (closingSegLengthFactor + 1),
                (closingSegLengthFactor * offset1.p0.y + s1.y) / (closingSegLengthFactor + 1)
            );
            segList.addPt(mid1);
        }
        else {
            // This branch is not expected to be used
            // except for testing purposes.
            // It is equivalent to the JTS 1.9 logic for
            // closing segments (which results in very poor
            // performance for large buffer distances)
            segList.addPt(s1);
        }

        // add start point of next segment offset
        segList.addPt(offset1.p0);
    }
}

/* private */
void
OffsetSegmentGenerator::addMitreJoin(const geom::Coordinate& p,
                                     const geom::LineSegment& p_offset0,
                                     const geom::LineSegment& p_offset1,
                                     double p_distance)
{
    /*
     * This computation is unstable if the offset segments are nearly collinear.
     * However, this situation should have been eliminated earlier by the check
     * for whether the offset segment endpoints are almost coincident
     */
    Coordinate intPt = algorithm::Intersection::intersection(offset0.p0, offset0.p1, offset1.p0, offset1.p1);
    if (!intPt.isNull()) {
        double mitreRatio = p_distance <= 0.0 ? 1.0 : intPt.distance(p) / fabs(p_distance);
        if (mitreRatio <= bufParams.getMitreLimit()) {
            segList.addPt(intPt);
            return;
        }
    }
    // at this point either intersection failed or mitre limit was exceeded
    addLimitedMitreJoin(p_offset0, p_offset1, p_distance,
                        bufParams.getMitreLimit());
}

/* private */
void
OffsetSegmentGenerator::addLimitedMitreJoin(
    const geom::LineSegment& p_offset0,
    const geom::LineSegment& p_offset1,
    double p_distance, double p_mitreLimit)
{
    ::geos::ignore_unused_variable_warning(p_offset0);
    ::geos::ignore_unused_variable_warning(p_offset1);

    const Coordinate& basePt = seg0.p1;

    double ang0 = Angle::angle(basePt, seg0.p0);
    //double ang1 = Angle::angle(basePt, seg1.p1); // unused in JTS, bug ?

    // oriented angle between segments
    double angDiff = Angle::angleBetweenOriented(seg0.p0, basePt, seg1.p1);
    // half of the interior angle
    double angDiffHalf = angDiff / 2;

    // angle for bisector of the interior angle between the segments
    double midAng = Angle::normalize(ang0 + angDiffHalf);
    // rotating this by PI gives the bisector of the reflex angle
    double mitreMidAng = Angle::normalize(midAng + PI);

    // the miterLimit determines the distance to the mitre bevel
    double mitreDist = p_mitreLimit * p_distance;
    // the bevel delta is the difference between the buffer distance
    // and half of the length of the bevel segment
    double bevelDelta = mitreDist * fabs(sin(angDiffHalf));
    double bevelHalfLen = p_distance - bevelDelta;

    // compute the midpoint of the bevel segment
    double bevelMidX = basePt.x + mitreDist * cos(mitreMidAng);
    double bevelMidY = basePt.y + mitreDist * sin(mitreMidAng);
    Coordinate bevelMidPt(bevelMidX, bevelMidY);

    // compute the mitre midline segment from the corner point to
    // the bevel segment midpoint
    LineSegment mitreMidLine(basePt, bevelMidPt);

    // finally the bevel segment endpoints are computed as offsets from
    // the mitre midline
    Coordinate bevelEndLeft;
    mitreMidLine.pointAlongOffset(1.0, bevelHalfLen, bevelEndLeft);
    Coordinate bevelEndRight;
    mitreMidLine.pointAlongOffset(1.0, -bevelHalfLen, bevelEndRight);

    if(side == Position::LEFT) {
        segList.addPt(bevelEndLeft);
        segList.addPt(bevelEndRight);
    }
    else {
        segList.addPt(bevelEndRight);
        segList.addPt(bevelEndLeft);
    }

}

/* private */
void
OffsetSegmentGenerator::addBevelJoin(const geom::LineSegment& p_offset0,
                                     const geom::LineSegment& p_offset1)
{
    segList.addPt(p_offset0.p1);
    segList.addPt(p_offset1.p0);
}

} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos

