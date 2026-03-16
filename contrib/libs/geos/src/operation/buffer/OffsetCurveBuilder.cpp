/**********************************************************************
 *
 * GEOS-Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009-2011  Sandro Santilli <strk@kbt.io>
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
 * Last port: operation/buffer/OffsetCurveBuilder.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <cassert>
#include <cmath>
#include <vector>

#include <geos/algorithm/Angle.h>
#include <geos/operation/buffer/OffsetCurveBuilder.h>
#include <geos/operation/buffer/BufferInputLineSimplifier.h>
#include <geos/operation/buffer/BufferOp.h>
#include <geos/operation/buffer/BufferParameters.h>
#include <geos/geom/Position.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/algorithm/NotRepresentableException.h>
#include <geos/algorithm/HCoordinate.h>
#include <geos/util.h>
#include <geos/util/IllegalArgumentException.h>

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
const double OffsetCurveBuilder::SIMPLIFY_FACTOR = 100.0;

/*public*/
void
OffsetCurveBuilder::getLineCurve(const CoordinateSequence* inputPts,
                                 double nDistance, vector<CoordinateSequence*>& lineList)
{
    distance = nDistance;

    if (isLineOffsetEmpty(distance)) {
        return;
    }

    double posDistance = std::abs(distance);

    std::unique_ptr<OffsetSegmentGenerator> segGen = getSegGen(posDistance);
    if(inputPts->getSize() <= 1) {
        computePointCurve(inputPts->getAt(0), *segGen);
    }
    else {
        if(bufParams.isSingleSided()) {
            bool isRightSide = distance < 0.0;
            computeSingleSidedBufferCurve(*inputPts, isRightSide, *segGen);
        }
        else {
            computeLineBufferCurve(*inputPts, *segGen);
        }
    }

    segGen->getCoordinates(lineList);
}

/* private */
void
OffsetCurveBuilder::computePointCurve(const Coordinate& pt,
                                      OffsetSegmentGenerator& segGen)
{
    switch(bufParams.getEndCapStyle()) {
    case BufferParameters::CAP_ROUND:
        segGen.createCircle(pt, distance);
        break;
    case BufferParameters::CAP_SQUARE:
        segGen.createSquare(pt, distance);
        break;
    default:
        // otherwise curve is empty (e.g. for a butt cap);
        break;
    }
}

/*public*/
void
OffsetCurveBuilder::getSingleSidedLineCurve(const CoordinateSequence* inputPts,
        double p_distance, vector<CoordinateSequence*>& lineList, bool leftSide,
        bool rightSide)
{
    // A zero or negative width buffer of a line/point is empty.
    if(p_distance <= 0.0) {
        return ;
    }

    if(inputPts->getSize() < 2) {
        // No cap, so just return.
        return ;
    }

    double distTol = simplifyTolerance(p_distance);

    std::unique_ptr<OffsetSegmentGenerator> segGen = getSegGen(p_distance);

    if(leftSide) {
        //--------- compute points for left side of line
        // Simplify the appropriate side of the line before generating
        std::unique_ptr<CoordinateSequence> simp1_ =
            BufferInputLineSimplifier::simplify(*inputPts, distTol);
        const CoordinateSequence& simp1 = *simp1_;


        auto n1 = simp1.size() - 1;
        if(! n1) {
            throw util::IllegalArgumentException("Cannot get offset of single-vertex line");
        }
        segGen->initSideSegments(simp1[0], simp1[1], Position::LEFT);
        segGen->addFirstSegment();
        for(size_t i = 2; i <= n1; ++i) {
            segGen->addNextSegment(simp1[i], true);
        }
        segGen->addLastSegment();
    }

    if(rightSide) {

        //---------- compute points for right side of line
        // Simplify the appropriate side of the line before generating
        std::unique_ptr<CoordinateSequence> simp2_ =
            BufferInputLineSimplifier::simplify(*inputPts, -distTol);
        const CoordinateSequence& simp2 = *simp2_;

        auto n2 = simp2.size() - 1;
        if(! n2) {
            throw util::IllegalArgumentException("Cannot get offset of single-vertex line");
        }
        segGen->initSideSegments(simp2[n2], simp2[n2 - 1], Position::LEFT);
        segGen->addFirstSegment();
        for(size_t i = n2 - 1; i > 0; --i) {
            segGen->addNextSegment(simp2[i - 1], true);
        }
        segGen->addLastSegment();
    }

    segGen->getCoordinates(lineList);
}

/*public*/
bool
OffsetCurveBuilder::isLineOffsetEmpty(double p_distance)
{
    // a zero width buffer of a line or point is empty
    if (p_distance == 0.0) return true;
    // a negative width buffer of a line or point is empty,
    // except for single-sided buffers, where the sign indicates the side
    if (p_distance < 0.0 && ! bufParams.isSingleSided()) return true;
    return false;
}


/*public*/
void
OffsetCurveBuilder::getRingCurve(const CoordinateSequence* inputPts,
                                 int side, double nDistance,
                                 vector<CoordinateSequence*>& lineList)
{
    distance = nDistance;

    // optimize creating ring for zero distance
    if(distance == 0.0) {
        lineList.push_back(inputPts->clone().release());
        return;
    }

    if(inputPts->getSize() <= 2) {
        getLineCurve(inputPts, distance, lineList);
        return;
    }

    std::unique_ptr<OffsetSegmentGenerator> segGen = getSegGen(std::abs(distance));
    computeRingBufferCurve(*inputPts, side, *segGen);
    segGen->getCoordinates(lineList);
}

/* private */
double
OffsetCurveBuilder::simplifyTolerance(double bufDistance)
{
    return bufDistance / SIMPLIFY_FACTOR;
}

/*private*/
void
OffsetCurveBuilder::computeLineBufferCurve(const CoordinateSequence& inputPts,
        OffsetSegmentGenerator& segGen)
{
    double distTol = simplifyTolerance(distance);

    //--------- compute points for left side of line
    // Simplify the appropriate side of the line before generating
    std::unique_ptr<CoordinateSequence> simp1_ =
        BufferInputLineSimplifier::simplify(inputPts, distTol);
    const CoordinateSequence& simp1 = *simp1_;


    auto n1 = simp1.size() - 1;
    segGen.initSideSegments(simp1[0], simp1[1], Position::LEFT);
    for(size_t i = 2; i <= n1; ++i) {
        segGen.addNextSegment(simp1[i], true);
    }
    segGen.addLastSegment();
    // add line cap for end of line
    segGen.addLineEndCap(simp1[n1 - 1], simp1[n1]);

    //---------- compute points for right side of line
    // Simplify the appropriate side of the line before generating
    std::unique_ptr<CoordinateSequence> simp2_ =
        BufferInputLineSimplifier::simplify(inputPts, -distTol);
    const CoordinateSequence& simp2 = *simp2_;

    auto n2 = simp2.size() - 1;
    segGen.initSideSegments(simp2[n2], simp2[n2 - 1], Position::LEFT);
    for(size_t i = n2 - 1; i > 0; --i) {
        segGen.addNextSegment(simp2[i - 1], true);
    }
    segGen.addLastSegment();
    // add line cap for start of line
    segGen.addLineEndCap(simp2[1], simp2[0]);

    segGen.closeRing();
}

/*private*/
void
OffsetCurveBuilder::computeRingBufferCurve(const CoordinateSequence& inputPts,
        int side, OffsetSegmentGenerator& segGen)
{
    // simplify input line to improve performance
    double distTol = simplifyTolerance(distance);
    // ensure that correct side is simplified
    if(side == Position::RIGHT) {
        distTol = -distTol;
    }
    std::unique_ptr<CoordinateSequence> simp_ =
        BufferInputLineSimplifier::simplify(inputPts, distTol);
    const CoordinateSequence& simp = *simp_;

    auto n = simp.size() - 1;
    segGen.initSideSegments(simp[n - 1], simp[0], side);
    for(size_t i = 1; i <= n; i++) {
        bool addStartPoint = i != 1;
        segGen.addNextSegment(simp[i], addStartPoint);
    }
    segGen.closeRing();
}

/*private*/
void
OffsetCurveBuilder::computeSingleSidedBufferCurve(
    const CoordinateSequence& inputPts, bool isRightSide,
    OffsetSegmentGenerator& segGen)
{
    double distTol = simplifyTolerance(distance);

    if(isRightSide) {

        // add original line
        segGen.addSegments(inputPts, true);

        //---------- compute points for right side of line
        // Simplify the appropriate side of the line before generating
        std::unique_ptr<CoordinateSequence> simp2_ =
            BufferInputLineSimplifier::simplify(inputPts, -distTol);
        const CoordinateSequence& simp2 = *simp2_;

        auto n2 = simp2.size() - 1;
        segGen.initSideSegments(simp2[n2], simp2[n2 - 1], Position::LEFT);
        segGen.addFirstSegment();
        for(size_t  i = n2 - 1; i > 0; --i) {
            segGen.addNextSegment(simp2[i - 1], true);
        }

    }
    else {

        // add original line
        segGen.addSegments(inputPts, false);

        //--------- compute points for left side of line
        // Simplify the appropriate side of the line before generating
        std::unique_ptr<CoordinateSequence> simp1_ =
            BufferInputLineSimplifier::simplify(inputPts, distTol);
        const CoordinateSequence& simp1 = *simp1_;

        auto n1 = simp1.size() - 1;
        segGen.initSideSegments(simp1[0], simp1[1], Position::LEFT);
        segGen.addFirstSegment();
        for(size_t i = 2; i <= n1; ++i) {
            segGen.addNextSegment(simp1[i], true);
        }

    }
    segGen.addLastSegment();
    segGen.closeRing();
}

/*private*/
std::unique_ptr<OffsetSegmentGenerator>
OffsetCurveBuilder::getSegGen(double dist)
{
    std::unique_ptr<OffsetSegmentGenerator> osg(
        new OffsetSegmentGenerator(precisionModel, bufParams, dist)
    );
    return osg;
}

} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos

