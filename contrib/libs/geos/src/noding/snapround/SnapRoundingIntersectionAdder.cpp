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

#include <geos/noding/snapround/SnapRoundingIntersectionAdder.h>
#include <geos/noding/snapround/HotPixel.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/noding/NodingValidator.h>
#include <geos/noding/IntersectionFinderAdder.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/algorithm/Distance.h>
#include <geos/util.h>

#include <vector>
#include <exception>
#include <iostream>
#include <cassert>

using namespace std;
using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding
namespace snapround { // geos.noding.snapround


SnapRoundingIntersectionAdder::SnapRoundingIntersectionAdder(const geom::PrecisionModel* newPm)
    : SegmentIntersector()
    , intersections(new std::vector<geom::Coordinate>)
    // , pm(newPm)
{
    double snapGridSize = 1.0 / newPm->getScale();
    nearnessTol = snapGridSize / NEARNESS_FACTOR;
}

/*public*/
void
SnapRoundingIntersectionAdder::processIntersections(
    SegmentString* e0, size_t segIndex0,
    SegmentString* e1, size_t segIndex1)
{
    // don't bother intersecting a segment with itself
    if (e0 == e1 && segIndex0 == segIndex1) return;

    const Coordinate& p00 = e0->getCoordinate(segIndex0);
    const Coordinate& p01 = e0->getCoordinate(segIndex0 + 1);
    const Coordinate& p10 = e1->getCoordinate(segIndex1);
    const Coordinate& p11 = e1->getCoordinate(segIndex1 + 1);

    li.computeIntersection(p00, p01, p10, p11);
    if (li.hasIntersection()) {
        if (li.isInteriorIntersection()) {
            for (std::size_t intIndex = 0, intNum = li.getIntersectionNum(); intIndex < intNum; intIndex++) {
                // Take a copy of the intersection coordinate
                intersections->emplace_back(li.getIntersection(intIndex));
            }
            static_cast<NodedSegmentString*>(e0)->addIntersections(&li, segIndex0, 0);
            static_cast<NodedSegmentString*>(e1)->addIntersections(&li, segIndex1, 1);
            return;
        }
    }

    /**
     * Segments did not actually intersect, within the limits of orientation index robustness.
     *
     * To avoid certain robustness issues in snap-rounding,
     * also treat very near vertex-segment situations as intersections.
     */
    processNearVertex(p00, e1, segIndex1, p10, p11 );
    processNearVertex(p01, e1, segIndex1, p10, p11 );
    processNearVertex(p10, e0, segIndex0, p00, p01 );
    processNearVertex(p11, e0, segIndex0, p00, p01 );
}

/*private*/
void
SnapRoundingIntersectionAdder::processNearVertex(
    const geom::Coordinate& p, SegmentString* edge, size_t segIndex,
    const geom::Coordinate& p0, const geom::Coordinate& p1)
{
    /**
     * Don't add intersection if candidate vertex is near endpoints of segment.
     * This avoids creating "zig-zag" linework
     * (since the vertex could actually be outside the segment envelope).
     */
    if (p.distance(p0) < nearnessTol) return;
    if (p.distance(p1) < nearnessTol) return;

    double distSeg = algorithm::Distance::pointToSegment(p, p0, p1);
    if (distSeg < nearnessTol) {
        intersections->emplace_back(p);
        static_cast<NodedSegmentString*>(edge)->addIntersection(p, segIndex);
    }
}



} // namespace geos.noding.snapround
} // namespace geos.noding
} // namespace geos
