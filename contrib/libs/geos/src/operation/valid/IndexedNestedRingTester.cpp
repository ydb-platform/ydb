/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/valid/IndexedNestedRingTester.java r399 (JTS-1.12)
 *
 **********************************************************************/

#include "IndexedNestedRingTester.h"

#include <geos/geom/LinearRing.h> // for use
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/operation/valid/IsValidOp.h> // for use (findPtNotNode)
#include <geos/index/strtree/STRtree.h> // for use
#include <geos/geom/Location.h>

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
class Envelope;
}
}

namespace geos {
namespace operation { // geos.operation
namespace valid { // geos.operation.valid

bool
IndexedNestedRingTester::isNonNested()
{
    buildIndex();

    std::vector<void*> results;
    for(size_t i = 0, n = rings.size(); i < n; ++i) {
        results.clear();

        const geom::LinearRing* outerRing = rings[i];

        geos::algorithm::locate::IndexedPointInAreaLocator locator(*outerRing);

        index->query(outerRing->getEnvelopeInternal(), results);
        for(const auto& result : results) {
            const geom::LinearRing* possibleInnerRing = static_cast<const geom::LinearRing*>(result);
            const geom::CoordinateSequence* possibleInnerRingPts = possibleInnerRing->getCoordinatesRO();

            if(outerRing == possibleInnerRing) {
                continue;
            }

            if(!outerRing->getEnvelopeInternal()->covers(possibleInnerRing->getEnvelopeInternal())) {
                continue;
            }

            const geom::Coordinate* innerRingPt =
                IsValidOp::findPtNotNode(possibleInnerRingPts,
                                         outerRing,
                                         graph);

            /*
             * If no non-node pts can be found, this means
             * that the possibleInnerRing touches ALL of the outerRing vertices.
             * This indicates an invalid polygon, since either
             * the two holes create a disconnected interior,
             * or they touch in an infinite number of points
             * (i.e. along a line segment).
             * Both of these cases are caught by other tests,
             * so it is safe to simply skip this situation here.
             */
            if(! innerRingPt) {
                continue;
            }

            bool isInside = locator.locate(innerRingPt) != geom::Location::EXTERIOR;

            if(isInside) {
                nestedPt = innerRingPt;
                return false;
            }

        }
    }

    return true;
}

IndexedNestedRingTester::~IndexedNestedRingTester()
{
    delete index;
}

void
IndexedNestedRingTester::buildIndex()
{
    delete index;

    index = new index::strtree::STRtree();
    for(size_t i = 0, n = rings.size(); i < n; ++i) {
        const geom::LinearRing* ring = rings[i];
        const geom::Envelope* env = ring->getEnvelopeInternal();
        index->insert(env, (void*)ring);
    }
}

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos
