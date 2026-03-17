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
 ***********************************************************************
 *
 * Last port: operation/overlay/EdgeSetNoder.java rev. 1.12 (JTS-1.10)
 *
 **********************************************************************/

#include <vector>

#include <geos/operation/overlay/EdgeSetNoder.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/index/EdgeSetIntersector.h>
#include <geos/geomgraph/index/SimpleMCSweepLineIntersector.h>
#include <geos/geomgraph/index/SegmentIntersector.h>

using namespace std;
using namespace geos::algorithm;
using namespace geos::geomgraph;
using namespace geos::geomgraph::index;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay


void
EdgeSetNoder::addEdges(vector<Edge*>* edges)
{
    inputEdges->insert(inputEdges->end(), edges->begin(), edges->end());
}

vector<Edge*>*
EdgeSetNoder::getNodedEdges()
{
    EdgeSetIntersector* esi = new SimpleMCSweepLineIntersector();
    SegmentIntersector* si = new SegmentIntersector(li, true, false);
    esi->computeIntersections(inputEdges, si, true);
    //Debug.println("has proper int = " + si.hasProperIntersection());
    vector<Edge*>* splitEdges = new vector<Edge*>();
    for(int i = 0; i < (int)inputEdges->size(); i++) {
        Edge* e = (*inputEdges)[i];
        e->getEdgeIntersectionList().addSplitEdges(splitEdges);
    }
    return splitEdges;
}

} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos
