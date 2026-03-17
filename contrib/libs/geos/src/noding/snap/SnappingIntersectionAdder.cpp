/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/noding/snap/SnappingIntersectionAdder.h>
#include <geos/noding/snap/SnappingPointIndex.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/algorithm/Distance.h>
#include <geos/util.h>

#include <vector>
#include <exception>
#include <cmath>
#include <iostream>

using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding
namespace snap { // geos.noding.snap


SnappingIntersectionAdder::SnappingIntersectionAdder(double p_snapTolerance, SnappingPointIndex& p_snapPointIndex)
    : SegmentIntersector()
    , snapTolerance(p_snapTolerance)
    , snapPointIndex(p_snapPointIndex) {}


/*public*/
void
SnappingIntersectionAdder::processIntersections(SegmentString* seg0, size_t segIndex0, SegmentString* seg1, size_t segIndex1)
{
    // don't bother intersecting a segment with itself
    if (seg0 == seg1 && segIndex0 == segIndex1) return;

    const Coordinate& p00 = seg0->getCoordinate(segIndex0);
    const Coordinate& p01 = seg0->getCoordinate(segIndex0 + 1);
    const Coordinate& p10 = seg1->getCoordinate(segIndex1);
    const Coordinate& p11 = seg1->getCoordinate(segIndex1 + 1);

    /**
     * Don't node intersections which are just
     * due to the shared vertex of adjacent segments.
     */
    if (!isAdjacent(seg0, segIndex0, seg1, segIndex1)) {
        li.computeIntersection(p00, p01, p10, p11);
        /**
         * Process single point intersections only.
         * Two-point (collinear) ones are handled by the near-vertex code
         */
        if (li.hasIntersection() && li.getIntersectionNum() == 1) {

            const Coordinate& intPt = li.getIntersection(0);
            const Coordinate& snapPt = snapPointIndex.snap(intPt);

            static_cast<NodedSegmentString*>(seg0)->addIntersection(snapPt, segIndex0);
            static_cast<NodedSegmentString*>(seg1)->addIntersection(snapPt, segIndex1);
        }
    }

    /**
     * The segments must also be snapped to the other segment endpoints.
     */
    processNearVertex(seg0, segIndex0, p00, seg1, segIndex1, p10, p11 );
    processNearVertex(seg0, segIndex0, p01, seg1, segIndex1, p10, p11 );
    processNearVertex(seg1, segIndex1, p10, seg0, segIndex0, p00, p01 );
    processNearVertex(seg1, segIndex1, p11, seg0, segIndex0, p00, p01 );
}


/*private*/
void
SnappingIntersectionAdder::processNearVertex(SegmentString* srcSS, size_t srcIndex, const geom::Coordinate& p,
        SegmentString* ss, size_t segIndex, const geom::Coordinate& p0, const geom::Coordinate& p1)
{
    /**
    * Don't add intersection if candidate vertex is near endpoints of segment.
    * This avoids creating "zig-zag" linework
    * (since the vertex could actually be outside the segment envelope).
    * Also, this should have already been snapped.
    */
    if (p.distance(p0) < snapTolerance) return;
    if (p.distance(p1) < snapTolerance) return;

    double distSeg = algorithm::Distance::pointToSegment(p, p0, p1);
    if (distSeg < snapTolerance) {
        // add node to target segment
        static_cast<NodedSegmentString*>(ss)->addIntersection(p, segIndex);
        // add node at vertex to source SS
        static_cast<NodedSegmentString*>(srcSS)->addIntersection(p, srcIndex);
    }
}


/*private static*/
bool
SnappingIntersectionAdder::isAdjacent(SegmentString* ss0, size_t segIndex0, SegmentString* ss1, size_t segIndex1)
{
    if (ss0 != ss1) return false;
    long l0 = segIndex0;
    long l1 = segIndex1;

    bool isAdjacent = (std::abs(l0 - l1) == 1);
    if (isAdjacent) {
        return true;
    }
    if (ss0->isClosed()) {
        std::size_t maxSegIndex = ss0->size();
        if ((segIndex0 == 0 && (segIndex1 + 1) == maxSegIndex) ||
            (segIndex1 == 0 && (segIndex0 + 1) == maxSegIndex)) {
            return true;
        }
    }
    return false;
}




} // namespace geos.noding.snap
} // namespace geos.noding
} // namespace geos
