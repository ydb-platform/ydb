/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2007-2010 Safe Software Inc.
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/valid/ConnectedInteriorTester.java rev. 1.15 (JTS-1.10)
 *
 **********************************************************************
 *
 * TODO:
 *
 *  - Remove heap allocation of GeometryFactory (might use a singleton)
 *  - Track MaximalEdgeRing references: we might be deleting them
 *    leaving dangling refs around.
 *
 **********************************************************************/

#include <geos/operation/valid/ConnectedInteriorTester.h>
#include <geos/operation/overlay/MaximalEdgeRing.h>
#include <geos/operation/overlay/MinimalEdgeRing.h>
#include <geos/operation/overlay/OverlayNodeFactory.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Location.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/LineString.h>
#include <geos/geomgraph/GeometryGraph.h>
#include <geos/geomgraph/PlanarGraph.h>
#include <geos/geomgraph/EdgeRing.h>
#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geom/Position.h>
#include <geos/geomgraph/Label.h>
#include <geos/util.h>

#include <vector>
#include <cassert>
#include <typeinfo>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
#include <iostream>
#endif

using namespace std;
using namespace geos::geom;
using namespace geos::geomgraph;
using namespace geos::operation::overlay;

namespace geos {
namespace operation { // geos.operation
namespace valid { // geos.operation.valid

ConnectedInteriorTester::ConnectedInteriorTester(GeometryGraph& newGeomGraph):
    geometryFactory(GeometryFactory::create()),
    geomGraph(newGeomGraph),
    disconnectedRingcoord()
{
}

Coordinate&
ConnectedInteriorTester::getCoordinate()
{
    return disconnectedRingcoord;
}

const Coordinate&
ConnectedInteriorTester::findDifferentPoint(const CoordinateSequence* coord,
        const Coordinate& pt)
{
    assert(coord);
    size_t npts = coord->getSize();
    for(size_t i = 0; i < npts; ++i) {
        if(!(coord->getAt(i) == pt)) {
            return coord->getAt(i);
        }
    }
    return Coordinate::getNull();
}

/*public*/
bool
ConnectedInteriorTester::isInteriorsConnected()
{

    // node the edges, in case holes touch the shell
    std::vector<Edge*> splitEdges;
    geomGraph.computeSplitEdges(&splitEdges);

    // form the edges into rings
    PlanarGraph graph(operation::overlay::OverlayNodeFactory::instance());

    graph.addEdges(splitEdges);
    setInteriorEdgesInResult(graph);
    graph.linkResultDirectedEdges();

    std::vector<EdgeRing*> edgeRings;
    buildEdgeRings(graph.getEdgeEnds(), edgeRings);

#if GEOS_DEBUG
    cerr << "buildEdgeRings constructed " << edgeRings.size() << " edgeRings." << endl;
#endif

    /*
     * Mark all the edges for the edgeRings corresponding to the shells
     * of the input polygons.
     *
     * Only ONE ring gets marked for each shell - if there are others
     * which remain unmarked this indicates a disconnected interior.
     */
    visitShellInteriors(geomGraph.getGeometry(), graph);

#if GEOS_DEBUG
    cerr << "after visitShellInteriors edgeRings are " << edgeRings.size() << " edgeRings." << endl;
#endif

    /*
     * If there are any unvisited shell edges
     * (i.e. a ring which is not a hole and which has the interior
     * of the parent area on the RHS)
     * this means that one or more holes must have split the interior of the
     * polygon into at least two pieces.  The polygon is thus invalid.
     */
    bool res = !hasUnvisitedShellEdge(&edgeRings);

#if GEOS_DEBUG
    cerr << "releasing " << edgeRings.size() << " edgeRings." << endl;
#endif
    // Release memory allocated by buildEdgeRings
    for(size_t i = 0, n = edgeRings.size(); i < n; ++i) {
        EdgeRing* er = edgeRings[i];
#if GEOS_DEBUG
        cerr << *er << endl;
#endif
        assert(er);
        delete er;
#if GEOS_DEBUG
        cerr << "releasing edgeRing at " << er << endl;
#endif
    }
    edgeRings.clear();

    // Release memory allocated by MaximalEdgeRings
    // There should be no more references to this object
    // how to check this ? boost::shared_ptr<> comes to mind.
    //
    for(size_t i = 0, n = maximalEdgeRings.size(); i < n; i++) {
        delete maximalEdgeRings[i];
    }
    maximalEdgeRings.clear();

    return res;
}

void
ConnectedInteriorTester::setInteriorEdgesInResult(PlanarGraph& graph)
{
    std::vector<EdgeEnd*>* ee = graph.getEdgeEnds();
    for(size_t i = 0, n = ee->size(); i < n; ++i) {
        // Unexpected non DirectedEdge in graphEdgeEnds
        DirectedEdge* de = detail::down_cast<DirectedEdge*>((*ee)[i]);
        if(de->getLabel().getLocation(0, Position::RIGHT) == Location::INTERIOR) {
            de->setInResult(true);
        }
    }
}

/*private*/
void
ConnectedInteriorTester::buildEdgeRings(std::vector<EdgeEnd*>* dirEdges,
                                        std::vector<EdgeRing*>& minEdgeRings)
{
#if GEOS_DEBUG
    cerr << __FUNCTION__ << " got " << dirEdges->size() << " EdgeEnd vector" << endl;
#endif

    typedef std::vector<EdgeEnd*> EdgeEnds;

    //std::vector<MinimalEdgeRing*> minEdgeRings;
    for(EdgeEnds::size_type i = 0, n = dirEdges->size(); i < n; ++i) {
        DirectedEdge* de = detail::down_cast<DirectedEdge*>((*dirEdges)[i]);

#if GEOS_DEBUG
        cerr << "DirectedEdge " << i << ": " << de->print() << endl;
#endif

        // if this edge has not yet been processed
        if(de->isInResult() && de->getEdgeRing() == nullptr) {
            MaximalEdgeRing* er = new MaximalEdgeRing(de,
                    geometryFactory.get());
            // We track MaximalEdgeRings allocations
            // using the private maximalEdgeRings vector
            maximalEdgeRings.push_back(er);

            er->linkDirectedEdgesForMinimalEdgeRings();
            er->buildMinimalRings(minEdgeRings);
        }
    }
    /*
    	std::vector<EdgeRing*> *edgeRings=new std::vector<EdgeRing*>();
    	edgeRings->assign(minEdgeRings.begin(), minEdgeRings.end());
    	return edgeRings;
    */
}

/**
 * Mark all the edges for the edgeRings corresponding to the shells
 * of the input polygons.  Note only ONE ring gets marked for each shell.
 */
void
ConnectedInteriorTester::visitShellInteriors(const Geometry* g, PlanarGraph& graph)
{
    if(const Polygon* p = dynamic_cast<const Polygon*>(g)) {
        visitInteriorRing(p->getExteriorRing(), graph);
    }

    if(const MultiPolygon* mp = dynamic_cast<const MultiPolygon*>(g)) {
        for(size_t i = 0, n = mp->getNumGeometries(); i < n; i++) {
            const Polygon* p = mp->getGeometryN(i);
            visitInteriorRing(p->getExteriorRing(), graph);
        }
    }
}

void
ConnectedInteriorTester::visitInteriorRing(const LineString* ring, PlanarGraph& graph)
{
    // can't visit an empty ring
    if(ring->isEmpty()) {
        return;
    }

    const CoordinateSequence* pts = ring->getCoordinatesRO();
    const Coordinate& pt0 = pts->getAt(0);

    /*
     * Find first point in coord list different to initial point.
     * Need special check since the first point may be repeated.
     */
    const Coordinate& pt1 = findDifferentPoint(pts, pt0);
    Edge* e = graph.findEdgeInSameDirection(pt0, pt1);
    DirectedEdge* de = static_cast<DirectedEdge*>(graph.findEdgeEnd(e));
    DirectedEdge* intDe = nullptr;
    if(de->getLabel().getLocation(0, Position::RIGHT) == Location::INTERIOR) {
        intDe = de;
    }
    else if(de->getSym()->getLabel().getLocation(0, Position::RIGHT) == Location::INTERIOR) {
        intDe = de->getSym();
    }
    assert(intDe != nullptr); // unable to find dirEdge with Interior on RHS
    visitLinkedDirectedEdges(intDe);
}


void
ConnectedInteriorTester::visitLinkedDirectedEdges(DirectedEdge* start)
{
    DirectedEdge* startDe = start;
    DirectedEdge* de = start;
    //Debug.println(de);
    do {
        // found null Directed Edge
        assert(de != nullptr);

        de->setVisited(true);
        de = de->getNext();
        //Debug.println(de);
    }
    while(de != startDe);
}

/*private*/
bool
ConnectedInteriorTester::hasUnvisitedShellEdge(std::vector<EdgeRing*>* edgeRings)
{

#if GEOS_DEBUG
    cerr << "hasUnvisitedShellEdge called with " << edgeRings->size() << " edgeRings." << endl;
#endif

    for(std::vector<EdgeRing*>::iterator
            it = edgeRings->begin(), itEnd = edgeRings->end();
            it != itEnd;
            ++it) {
        EdgeRing* er = *it;
        assert(er);

        // don't check hole rings
        if(er->isHole()) {
            continue;
        }

        std::vector<DirectedEdge*>& edges = er->getEdges();
        DirectedEdge* de = edges[0];
        assert(de);

        // don't check CW rings which are holes
        // (MD - this check may now be irrelevant - 2006-03-09)
        if(de->getLabel().getLocation(0, Position::RIGHT) != Location::INTERIOR) {
            continue;
        }

        /*
         * the edgeRing is CW ring which surrounds the INT
         * of the area, so check all edges have been visited.
         * If any are unvisited, this is a disconnected part
         * of the interior
         */
        for(std::vector<DirectedEdge*>::iterator
                jt = edges.begin(), jtEnd = edges.end();
                jt != jtEnd;
                ++jt) {
            de = *jt;
            assert(de);
            //Debug.print("visted? "); Debug.println(de);
            if(!de->isVisited()) {
                //Debug.print("not visited "); Debug.println(de);
                disconnectedRingcoord = de->getCoordinate();
                return true;
            }
        }
    }
    return false;
}

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos
