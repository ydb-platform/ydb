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
 **********************************************************************/

#include <vector>

#include <geos/geomgraph/index/MonotoneChainIndexer.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Quadrant.h>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph
namespace index { // geos.geomgraph.index

void
MonotoneChainIndexer::getChainStartIndices(const CoordinateSequence* pts,
        vector<size_t>& startIndexList)
{
    // find the startpoint (and endpoints) of all monotone chains
    // in this edge
    size_t start = 0;
    //vector<int>* startIndexList=new vector<int>();
    startIndexList.push_back(start);
    do {
        auto last = findChainEnd(pts, start);
        startIndexList.push_back(last);
        start = last;
    }
    while(start < pts->size() - 1);
    // copy list to an array of ints, for efficiency
    //return startIndexList;
}

/**
 * @return the index of the last point in the monotone chain
 */
size_t
MonotoneChainIndexer::findChainEnd(const CoordinateSequence* pts, size_t start)
{
    // determine quadrant for chain
    auto chainQuad = Quadrant::quadrant(pts->getAt(start), pts->getAt(start + 1));
    auto last = start + 1;
    auto sz = pts->size(); // virtual call, doesn't inline
    while(last < sz) {
        // compute quadrant for next possible segment in chain
        auto quad = Quadrant::quadrant(pts->getAt(last - 1), pts->getAt(last));
        if(quad != chainQuad) {
            break;
        }
        ++last;
    }
    return last - 1;
}

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespage geos
