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
 * Last port: operation/valid/SweeplineNestedRingTester.java rev. 1.12 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/valid/SweeplineNestedRingTester.h>
#include <geos/operation/valid/IsValidOp.h>
#include <geos/index/sweepline/SweepLineInterval.h>
#include <geos/index/sweepline/SweepLineIndex.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/geom/LinearRing.h>

#include <cassert>

using namespace geos::algorithm;
using namespace geos::index::sweepline;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace valid { // geos.operation.valid

SweeplineNestedRingTester::OverlapAction::OverlapAction(SweeplineNestedRingTester* p)
{
    isNonNested = true;
    parent = p;
}

void
SweeplineNestedRingTester::OverlapAction::overlap(SweepLineInterval* s0, SweepLineInterval* s1)
{
    LinearRing* innerRing = (LinearRing*) s0->getItem();
    LinearRing* searchRing = (LinearRing*) s1->getItem();
    if(innerRing == searchRing) {
        return;
    }
    if(parent->isInside(innerRing, searchRing)) {
        isNonNested = false;
    }
}


bool
SweeplineNestedRingTester::isNonNested()
{
    buildIndex();
    OverlapAction* action = new OverlapAction(this);
    sweepLine->computeOverlaps(action);
    return action->isNonNested;
}

void
SweeplineNestedRingTester::buildIndex()
{
    sweepLine = new SweepLineIndex();
    for(size_t i = 0, n = rings.size(); i < n; i++) {
        LinearRing* ring = rings[i];
        const Envelope* env = ring->getEnvelopeInternal();
        SweepLineInterval* sweepInt = new SweepLineInterval(env->getMinX(), env->getMaxX(), ring);
        sweepLine->add(sweepInt);
    }
}

bool
SweeplineNestedRingTester::isInside(LinearRing* innerRing, LinearRing* searchRing)
{
    const CoordinateSequence* innerRingPts = innerRing->getCoordinatesRO();
    const CoordinateSequence* searchRingPts = searchRing->getCoordinatesRO();

    if(!innerRing->getEnvelopeInternal()->intersects(searchRing->getEnvelopeInternal())) {
        return false;
    }
    const Coordinate* innerRingPt = IsValidOp::findPtNotNode(innerRingPts, searchRing, graph);

    // Unable to find a ring point not a node of the search ring
    assert(innerRingPt != nullptr);

    bool p_isInside = PointLocation::isInRing(*innerRingPt, searchRingPts);
    if(p_isInside) {
        /*
         * innerRingPt is const just because the input
         * CoordinateSequence is const. If the input
         * Polygon survives lifetime of this object
         * we are safe.
         */
        nestedPt = const_cast<Coordinate*>(innerRingPt);
        return true;
    }
    return false;
}

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos

