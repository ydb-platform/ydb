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
#include <algorithm>

#include <geos/geomgraph/index/SimpleSweepLineIntersector.h>
#include <geos/geomgraph/index/SweepLineEvent.h>
#include <geos/geomgraph/index/SweepLineSegment.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geomgraph/Edge.h>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph
namespace index { // geos.geomgraph.index

SimpleSweepLineIntersector::SimpleSweepLineIntersector():
    //events(new vector<SweepLineEvent*>()),
    nOverlaps(0)
{
}

SimpleSweepLineIntersector::~SimpleSweepLineIntersector()
{
    for(unsigned int i = 0; i < events.size(); ++i) {
        delete events[i];
    }
}

void
SimpleSweepLineIntersector::computeIntersections(vector<Edge*>* edges,
        SegmentIntersector* si, bool testAllSegments)
{
    if(testAllSegments) {
        add(edges, nullptr);
    }
    else {
        add(edges);
    }
    computeIntersections(si);
}

void
SimpleSweepLineIntersector::computeIntersections(vector<Edge*>* edges0, vector<Edge*>* edges1, SegmentIntersector* si)
{
    add(edges0, edges0);
    add(edges1, edges1);
    computeIntersections(si);
}

void
SimpleSweepLineIntersector::add(vector<Edge*>* edges)
{
    for(unsigned int i = 0; i < edges->size(); ++i) {
        Edge* edge = (*edges)[i];
        // edge is its own group
        add(edge, edge);
    }
}

void
SimpleSweepLineIntersector::add(vector<Edge*>* edges, void* edgeSet)
{
    for(unsigned int i = 0; i < edges->size(); ++i) {
        Edge* edge = (*edges)[i];
        add(edge, edgeSet);
    }
}

void
SimpleSweepLineIntersector::add(Edge* edge, void* edgeSet)
{
    const CoordinateSequence* pts = edge->getCoordinates();
    std::size_t n = pts->getSize() - 1;
    for(std::size_t i = 0; i < n; ++i) {
        SweepLineSegment* ss = new SweepLineSegment(edge, i);
        SweepLineEvent* insertEvent = new SweepLineEvent(edgeSet, ss->getMinX(), nullptr, ss);
        events.push_back(insertEvent);
        events.push_back(new SweepLineEvent(edgeSet, ss->getMaxX(), insertEvent, ss));
    }
}

/**
 * Because Delete Events have a link to their corresponding Insert event,
 * it is possible to compute exactly the range of events which must be
 * compared to a given Insert event object.
 */
void
SimpleSweepLineIntersector::prepareEvents()
{
    sort(events.begin(), events.end(), SweepLineEventLessThen());
    for(unsigned int i = 0; i < events.size(); ++i) {
        SweepLineEvent* ev = events[i];
        if(ev->isDelete()) {
            ev->getInsertEvent()->setDeleteEventIndex(i);
        }
    }
}

void
SimpleSweepLineIntersector::computeIntersections(SegmentIntersector* si)
{
    nOverlaps = 0;
    prepareEvents();
    for(unsigned int i = 0; i < events.size(); ++i) {
        SweepLineEvent* ev = events[i];
        if(ev->isInsert()) {
            processOverlaps(i, ev->getDeleteEventIndex(), ev, si);
        }
    }
}

void
SimpleSweepLineIntersector::processOverlaps(size_t start, size_t end, SweepLineEvent* ev0,
        SegmentIntersector* si)
{

    SweepLineSegment* ss0 = (SweepLineSegment*) ev0->getObject();

    /*
     * Since we might need to test for self-intersections,
     * include current insert event object in list of event objects to test.
     * Last index can be skipped, because it must be a Delete event.
     */
    for(auto i = start; i < end; ++i) {
        SweepLineEvent* ev1 = events[i];
        if(ev1->isInsert()) {
            SweepLineSegment* ss1 = (SweepLineSegment*) ev1->getObject();
            if(ev0->edgeSet == nullptr || (ev0->edgeSet != ev1->edgeSet)) {
                ss0->computeIntersections(ss1, si);
                nOverlaps++;
            }
        }
    }
}

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos
