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

#include <vector>

#include <geos/geomgraph/index/SimpleEdgeSetIntersector.h>
#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geomgraph/Edge.h>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph
namespace index { // geos.geomgraph.index

SimpleEdgeSetIntersector::SimpleEdgeSetIntersector():
    nOverlaps(0)
{
}

void
SimpleEdgeSetIntersector::computeIntersections(vector<Edge*>* edges,
        SegmentIntersector* si, bool testAllSegments)
{
    nOverlaps = 0;
    size_t nedges = edges->size();
    for(size_t i0 = 0; i0 < nedges; ++i0) {
        Edge* edge0 = (*edges)[i0];
        for(size_t i1 = 0; i1 < nedges; ++i1) {
            Edge* edge1 = (*edges)[i1];
            if(testAllSegments || edge0 != edge1) {
                computeIntersects(edge0, edge1, si);
            }
        }
    }
}


void
SimpleEdgeSetIntersector::computeIntersections(vector<Edge*>* edges0,
        vector<Edge*>* edges1, SegmentIntersector* si)
{
    nOverlaps = 0;

    size_t nedges0 = edges0->size();
    size_t nedges1 = edges1->size();

    for(size_t i0 = 0; i0 < nedges0; ++i0) {
        Edge* edge0 = (*edges0)[i0];
        for(size_t i1 = 0; i1 < nedges1; ++i1) {
            Edge* edge1 = (*edges1)[i1];
            computeIntersects(edge0, edge1, si);
        }
    }
}

/**
 * Performs a brute-force comparison of every segment in each Edge.
 * This has n^2 performance, and is about 100 times slower than using
 * monotone chains.
 */
void
SimpleEdgeSetIntersector::computeIntersects(Edge* e0, Edge* e1,
        SegmentIntersector* si)
{
    const CoordinateSequence* pts0 = e0->getCoordinates();
    const CoordinateSequence* pts1 = e1->getCoordinates();

    auto npts0 = pts0->size();
    auto npts1 = pts1->size();

    for(size_t i0 = 0; i0 < npts0 - 1; ++i0) {
        for(size_t i1 = 0; i1 < npts1 - 1; ++i1) {
            si->addIntersections(e0, i0, e1, i1);
        }
    }
}

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos
