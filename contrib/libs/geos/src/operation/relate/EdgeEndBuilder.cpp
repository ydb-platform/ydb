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
 * Last port: operation/relate/EdgeEndBuilder.java rev. 1.12 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/relate/EdgeEndBuilder.h>
#include <geos/geom/Coordinate.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/EdgeEnd.h>
#include <geos/geomgraph/EdgeIntersectionList.h>
#include <geos/geomgraph/Label.h>

#include <vector>

using namespace geos::geomgraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace relate { // geos.operation.relate

std::vector<EdgeEnd*>
EdgeEndBuilder::computeEdgeEnds(std::vector<Edge*>* edges)
{
    std::vector<EdgeEnd*> l;
    for(Edge* e : *edges) {
        computeEdgeEnds(e, &l);
    }
    return l;
}

/**
 * Creates stub edges for all the intersections in this
 * Edge (if any) and inserts them into the graph.
 */
void
EdgeEndBuilder::computeEdgeEnds(Edge* edge, std::vector<EdgeEnd*>* l)
{
    EdgeIntersectionList& eiList = edge->getEdgeIntersectionList();
    //Debug.print(eiList);
    // ensure that the list has entries for the first and last point of the edge
    eiList.addEndpoints();

    EdgeIntersectionList::const_iterator it = eiList.begin();
    // no intersections, so there is nothing to do
    if(it == eiList.end()) {
        return;
    }

    const EdgeIntersection* eiPrev = nullptr;
    const EdgeIntersection* eiCurr = nullptr;

    const EdgeIntersection* eiNext = &*it;
    ++it;
    do {
        eiPrev = eiCurr;
        eiCurr = eiNext;
        eiNext = nullptr;
        if(it != eiList.end()) {
            eiNext = &*it;
            ++it;
        }
        if(eiCurr != nullptr) {
            createEdgeEndForPrev(edge, l, eiCurr, eiPrev);
            createEdgeEndForNext(edge, l, eiCurr, eiNext);
        }
    }
    while(eiCurr != nullptr);
}

/**
 * Create a EdgeStub for the edge before the intersection eiCurr.
 * The previous intersection is provided
 * in case it is the endpoint for the stub edge.
 * Otherwise, the previous point from the parent edge will be the endpoint.
 *
 * eiCurr will always be an EdgeIntersection, but eiPrev may be null.
 */
void
EdgeEndBuilder::createEdgeEndForPrev(Edge* edge, std::vector<EdgeEnd*>* l,
                                     const EdgeIntersection* eiCurr, const EdgeIntersection* eiPrev)
{
    auto iPrev = eiCurr->segmentIndex;
    if(eiCurr->dist == 0.0) {
        // if at the start of the edge there is no previous edge
        if(iPrev == 0) {
            return;
        }
        iPrev--;
    }
    Coordinate pPrev(edge->getCoordinate(iPrev));
    // if prev intersection is past the previous vertex, use it instead
    if(eiPrev != nullptr && eiPrev->segmentIndex >= iPrev) {
        pPrev = eiPrev->coord;
    }
    Label label(edge->getLabel());
    // since edgeStub is oriented opposite to it's parent edge, have to flip sides for edge label
    label.flip();
    EdgeEnd* e = new EdgeEnd(edge, eiCurr->coord, pPrev, label);
    //e.print(System.out);  System.out.println();
    l->push_back(e);
}

/**
 * Create a StubEdge for the edge after the intersection eiCurr.
 * The next intersection is provided
 * in case it is the endpoint for the stub edge.
 * Otherwise, the next point from the parent edge will be the endpoint.
 *
 * eiCurr will always be an EdgeIntersection, but eiNext may be null.
 */
void
EdgeEndBuilder::createEdgeEndForNext(Edge* edge, std::vector<EdgeEnd*>* l,
                                     const EdgeIntersection* eiCurr, const EdgeIntersection* eiNext)
{
    size_t iNext = eiCurr->segmentIndex + 1;
    // if there is no next edge there is nothing to do
    if(iNext >= edge->getNumPoints() && eiNext == nullptr) {
        return;
    }
    Coordinate pNext(edge->getCoordinate(iNext));
    // if the next intersection is in the same segment as the current, use it as the endpoint
    if(eiNext != nullptr && eiNext->segmentIndex == eiCurr->segmentIndex) {
        pNext = eiNext->coord;
    }
    EdgeEnd* e = new EdgeEnd(edge, eiCurr->coord, pNext, edge->getLabel());
    //Debug.println(e);
    l->push_back(e);
}

} // namespace geos.operation.relate
} // namespace geos.operation
} // namespace geos

