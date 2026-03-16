/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/IntersectionFinderAdder.java rev. 1.5 (JTS-1.9)
 *
 **********************************************************************/

#include <vector>

#include <geos/noding/IntersectionFinderAdder.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/geom/Coordinate.h>
#include <geos/util.h>

using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding

void
IntersectionFinderAdder::processIntersections(
    SegmentString* e0,  size_t segIndex0,
    SegmentString* e1,  size_t segIndex1)
{
    // don't bother intersecting a segment with itself
    if(e0 == e1 && segIndex0 == segIndex1) {
        return;
    }

    const Coordinate& p00 = e0->getCoordinate(segIndex0);
    const Coordinate& p01 = e0->getCoordinate(segIndex0 + 1);
    const Coordinate& p10 = e1->getCoordinate(segIndex1);
    const Coordinate& p11 = e1->getCoordinate(segIndex1 + 1);

    li.computeIntersection(p00, p01, p10, p11);
//if (li.hasIntersection() && li.isProper()) Debug.println(li);

    if(li.hasIntersection()) {
        if(li.isInteriorIntersection()) {
            for(size_t intIndex = 0, n = li.getIntersectionNum(); intIndex < n; ++intIndex) {
                interiorIntersections.push_back(li.getIntersection(intIndex));
            }

            NodedSegmentString* ee0 = detail::down_cast<NodedSegmentString*>(e0);
            NodedSegmentString* ee1 = detail::down_cast<NodedSegmentString*>(e1);
            ee0->addIntersections(&li, segIndex0, 0);
            ee1->addIntersections(&li, segIndex1, 1);
        }
    }
}

} // namespace geos.noding
} // namespace geos
