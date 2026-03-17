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
 * Last port: operation/valid/SimpleNestedRingTester.java rev. 1.14 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/valid/SimpleNestedRingTester.h>
#include <geos/operation/valid/IsValidOp.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Envelope.h>

#include <cassert>

using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace valid { // geos.operation.valid

bool
SimpleNestedRingTester::isNonNested()
{
    for(size_t i = 0, ni = rings.size(); i < ni; i++) {
        LinearRing* innerRing = rings[i];
        const CoordinateSequence* innerRingPts = innerRing->getCoordinatesRO();
        for(size_t j = 0, nj = rings.size(); j < nj; j++) {
            LinearRing* searchRing = rings[j];
            const CoordinateSequence* searchRingPts = searchRing->getCoordinatesRO();
            if(innerRing == searchRing) {
                continue;
            }
            if(!innerRing->getEnvelopeInternal()->intersects(searchRing->getEnvelopeInternal())) {
                continue;
            }
            const Coordinate* innerRingPt = IsValidOp::findPtNotNode(innerRingPts, searchRing, graph);
            // Unable to find a ring point not a node of the search ring
            assert(innerRingPt != nullptr);

            bool isInside = PointLocation::isInRing(*innerRingPt, searchRingPts);
            if(isInside) {
                /*
                 * innerRingPt is const just because the input
                 * CoordinateSequence is const. If the input
                 * Polygon survives lifetime of this object
                 * we are safe.
                 */
                nestedPt = const_cast<Coordinate*>(innerRingPt);
                return false;
            }
        }
    }
    return true;
}

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos

