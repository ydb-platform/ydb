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
 **********************************************************************/


#include <stdlib.h>
#include <vector>

#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/Node.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

#ifndef GEOS_INLINE
# include <geos/geomgraph/index/SegmentIntersector.inl>
#endif


#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifndef DEBUG_INTERSECT
#define DEBUG_INTERSECT 0
#endif

#if GEOS_DEBUG || DEBUG_INTERSECT
#include <iostream>
#endif

using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph
namespace index { // geos.geomgraph.index

using namespace geos::algorithm;

/*
 * A trivial intersection is an apparent self-intersection which in fact
 * is simply the point shared by adjacent line segments.
 * Note that closed edges require a special check for the point
 * shared by the beginning and end segments.
 */
bool
SegmentIntersector::isTrivialIntersection(Edge* e0,
        size_t segIndex0, Edge* e1,
        size_t segIndex1)
{
//	if (e0->equals(e1))
    if(e0 == e1) {
        if(li->getIntersectionNum() == 1) {
            if(isAdjacentSegments(segIndex0, segIndex1)) {
                return true;
            }
            if(e0->isClosed()) {
                auto maxSegIndex = e0->getNumPoints() - 1;
                if((segIndex0 == 0 && segIndex1 == maxSegIndex)
                        || (segIndex1 == 0 && segIndex0 == maxSegIndex)) {
                    return true;
                }
            }
        }
    }
    return false;
}

/**
 * This method is called by clients of the EdgeIntersector class to test
 * for and add intersections for two segments of the edges being intersected.
 * Note that clients (such as MonotoneChainEdges) may choose not to intersect
 * certain pairs of segments for efficiency reasons.
 */
void
SegmentIntersector::addIntersections(Edge* e0, size_t segIndex0, Edge* e1, size_t segIndex1)
{

#if GEOS_DEBUG
    cerr << "SegmentIntersector::addIntersections() called" << endl;
#endif

//	if (e0->equals(e1) && segIndex0==segIndex1) return;
    if(e0 == e1 && segIndex0 == segIndex1) {
        return;
    }
    numTests++;
    const CoordinateSequence* cl0 = e0->getCoordinates();
    const Coordinate& p00 = cl0->getAt(segIndex0);
    const Coordinate& p01 = cl0->getAt(segIndex0 + 1);
    const CoordinateSequence* cl1 = e1->getCoordinates();
    const Coordinate& p10 = cl1->getAt(segIndex1);
    const Coordinate& p11 = cl1->getAt(segIndex1 + 1);
    li->computeIntersection(p00, p01, p10, p11);

    /*
     * Always record any non-proper intersections.
     * If includeProper is true, record any proper intersections as well.
     */
    if(li->hasIntersection()) {
        if(recordIsolated) {
            e0->setIsolated(false);
            e1->setIsolated(false);
        }
        //intersectionFound = true;
        numIntersections++;

        // If the segments are adjacent they have at least one trivial
        // intersection, the shared endpoint.
        // Don't bother adding it if it is the
        // only intersection.
        if(!isTrivialIntersection(e0, segIndex0, e1, segIndex1)) {
#if GEOS_DEBUG
            cerr << "SegmentIntersector::addIntersections(): has !TrivialIntersection" << endl;
#endif // DEBUG_INTERSECT
            hasIntersectionVar = true;
            bool isBdyPt = isBoundaryPoint(li, bdyNodes);
            bool isNonProper = isBdyPt || !li->isProper();
            if(includeProper || isNonProper) {
                //Debug.println(li);
                e0->addIntersections(li, segIndex0, 0);
                e1->addIntersections(li, segIndex1, 1);
#if GEOS_DEBUG
                cerr << "SegmentIntersector::addIntersections(): includeProper || !li->isProper()" << endl;
#endif // DEBUG_INTERSECT
            }
            if(li->isProper()) {
                properIntersectionPoint = li->getIntersection(0);
#if GEOS_DEBUG
                cerr << "SegmentIntersector::addIntersections(): properIntersectionPoint: " << properIntersectionPoint.toString() <<
                     endl;
#endif // DEBUG_INTERSECT
                hasProper = true;
                if(isDoneWhenProperInt) {
                    isDone = true;
                }
                if(! isBdyPt) {
                    hasProperInterior = true;
                }
            }
        }
    }
}

/*private*/
bool
SegmentIntersector::isBoundaryPoint(LineIntersector* p_li,
                                    std::vector<Node*>* tstBdyNodes)
{
    if(! tstBdyNodes) {
        return false;
    }

    for(const Node* node: *tstBdyNodes) {
        const Coordinate& pt = node->getCoordinate();
        if(p_li->isIntersection(pt)) {
            return true;
        }
    }
    return false;
}


} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos
