/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/IteratedNoder.java r591 (JTS-1.12+)
 *
 **********************************************************************/

#include <sstream>
#include <vector>

#include <geos/profiler.h>
#include <geos/util/TopologyException.h>
#include <geos/noding/IteratedNoder.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/MCIndexNoder.h>
#include <geos/noding/IntersectionAdder.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding

/* private */
void
IteratedNoder::node(std::vector<SegmentString*>* segStrings,
                    int& numInteriorIntersections,
                    Coordinate& intersectionPoint)
{
    IntersectionAdder si(li);
    MCIndexNoder noder;
    noder.setSegmentIntersector(&si);
    noder.computeNodes(segStrings);
    nodedSegStrings = noder.getNodedSubstrings();
    numInteriorIntersections = si.numInteriorIntersections;

    if (si.hasProperInteriorIntersection()) {
        intersectionPoint = si.getProperIntersectionPoint();
    }
}

/* public */
void
IteratedNoder::computeNodes(SegmentString::NonConstVect* segStrings)
// throw(GEOSException);
{
    int numInteriorIntersections;
    nodedSegStrings = segStrings;
    int nodingIterationCount = 0;
    int lastNodesCreated = -1;
    std::vector<SegmentString*>* lastStrings = nullptr;
    Coordinate intersectionPoint = Coordinate::getNull();

    do {
        // NOTE: will change this.nodedSegStrings
        node(nodedSegStrings, numInteriorIntersections, intersectionPoint);

        // Delete noded strings from previous iteration
        if(lastStrings) {
            for(auto& s : *lastStrings) {
                delete s;
            }
            delete lastStrings;
        }
        lastStrings = nodedSegStrings;

        nodingIterationCount++;
        int nodesCreated = numInteriorIntersections;

        /*
         * Fail if the number of nodes created is not declining.
         * However, allow a few iterations at least before doing this
         */
        //cerr<<"# nodes created: "<<nodesCreated<<endl;
        if(lastNodesCreated > 0
                && nodesCreated >= lastNodesCreated
                && nodingIterationCount > maxIter) {

            // Delete noded strings from previous iteration
            if(lastStrings) {
                for(auto& s : *lastStrings) {
                    delete s;
                }
                delete lastStrings;
            }

            std::stringstream s;
            s << "Iterated noding failed to converge after " <<
              nodingIterationCount << " iterations (near " <<
              intersectionPoint << ")";
            throw util::TopologyException(s.str());
        }
        lastNodesCreated = nodesCreated;

    }
    while(lastNodesCreated > 0);
    //cerr<<"# nodings = "<<nodingIterationCount<<endl;
}


} // namespace geos.noding
} // namespace geos

