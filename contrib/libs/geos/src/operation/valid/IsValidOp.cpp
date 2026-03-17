/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010 Safe Software Inc.
 * Copyright (C) 2010 Sandro Santilli <strk@kbt.io>
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
 * Last port: operation/valid/IsValidOp.java r335 (JTS-1.12)
 *
 **********************************************************************/

#include "IndexedNestedRingTester.h"

#include <geos/export.h>
#include <geos/constants.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Location.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geomgraph/GeometryGraph.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/index/chain/MonotoneChainSelectAction.h>
#include <geos/operation/valid/ConnectedInteriorTester.h>
#include <geos/operation/valid/ConsistentAreaTester.h>
#include <geos/operation/valid/IsValidOp.h>
#include <geos/operation/valid/IndexedNestedShellTester.h>
#include <geos/util/UnsupportedOperationException.h>


#include <cassert>
#include <cmath>
#include <typeinfo>
#include <set>

using namespace std;
using namespace geos::algorithm;
using namespace geos::geomgraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace valid { // geos.operation.valid

/**
 * Find a point from the list of testCoords
 * that is NOT a node in the edge for the list of searchCoords
 *
 * @return the point found, or <code>null</code> if none found
 */
const Coordinate*
IsValidOp::findPtNotNode(const CoordinateSequence* testCoords,
                         const LinearRing* searchRing, const GeometryGraph* graph)
{
    // find edge corresponding to searchRing.
    Edge* searchEdge = graph->findEdge(searchRing);
    // find a point in the testCoords which is not a node of the searchRing
    EdgeIntersectionList& eiList = searchEdge->getEdgeIntersectionList();
    // somewhat inefficient - is there a better way? (Use a node map, for instance?)
    auto npts = testCoords->getSize();
    for(unsigned int i = 0; i < npts; ++i) {
        const Coordinate& pt = testCoords->getAt(i);
        if(!eiList.isIntersection(pt)) {
            return &pt;
        }
    }
    return nullptr;
}


bool
IsValidOp::isValid()
{
    checkValid();
    return validErr == nullptr;
}

/* static public */
bool
IsValidOp::isValid(const Coordinate& coord)
{
    if(! std::isfinite(coord.x)) {
        return false;
    }
    if(! std::isfinite(coord.y)) {
        return false;
    }
    return true;
}

/* static public */
bool
IsValidOp::isValid(const Geometry& g)
{
    IsValidOp op(&g);
    return op.isValid();
}

TopologyValidationError*
IsValidOp::getValidationError()
{
    checkValid();
    return validErr;
}

void
IsValidOp::checkValid()
{
    if(isChecked) {
        return;
    }
    checkValid(parentGeometry);
    isChecked = true;
}

void
IsValidOp::checkValid(const Geometry* g)
{
    assert(validErr == nullptr);

    if(nullptr == g) {
        return;
    }

    // empty geometries are always valid!
    if(g->isEmpty()) {
        return;
    }

    if(const Point* x1 = dynamic_cast<const Point*>(g)) {
        checkValid(x1);
    }
    // LineString also handles LinearRings, so we check LinearRing first
    else if(const LinearRing* x2 = dynamic_cast<const LinearRing*>(g)) {
        checkValid(x2);
    }
    else if(const LineString* x3 = dynamic_cast<const LineString*>(g)) {
        checkValid(x3);
    }
    else if(const Polygon* x4 = dynamic_cast<const Polygon*>(g)) {
        checkValid(x4);
    }
    else if(const MultiPolygon* x5 = dynamic_cast<const MultiPolygon*>(g)) {
        checkValid(x5);
    }
    else if(const GeometryCollection* x6 =
                dynamic_cast<const GeometryCollection*>(g)) {
        checkValid(x6);
    }
    else {
        throw util::UnsupportedOperationException();
    }
}

/*
 * Checks validity of a Point.
 */
void
IsValidOp::checkValid(const Point* g)
{
    checkInvalidCoordinates(g->getCoordinatesRO());
}

/*
 * Checks validity of a LineString.  Almost anything goes for linestrings!
 */
void
IsValidOp::checkValid(const LineString* g)
{
    checkInvalidCoordinates(g->getCoordinatesRO());
    if(validErr != nullptr) {
        return;
    }

    GeometryGraph graph(0, g);
    checkTooFewPoints(&graph);
}

/**
 * Checks validity of a LinearRing.
 */
void
IsValidOp::checkValid(const LinearRing* g)
{
    checkInvalidCoordinates(g->getCoordinatesRO());
    if(validErr != nullptr) {
        return;
    }

    checkClosedRing(g);
    if(validErr != nullptr) {
        return;
    }

    GeometryGraph graph(0, g);
    checkTooFewPoints(&graph);
    if(validErr != nullptr) {
        return;
    }

    LineIntersector li;
    graph.computeSelfNodes(&li, true, true);
    checkNoSelfIntersectingRings(&graph);
}

/**
 * Checks the validity of a polygon.
 * Sets the validErr flag.
 */
void
IsValidOp::checkValid(const Polygon* g)
{
    checkInvalidCoordinates(g);
    if(validErr != nullptr) {
        return;
    }

    checkClosedRings(g);
    if(validErr != nullptr) {
        return;
    }

    GeometryGraph graph(0, g);

    checkTooFewPoints(&graph);
    if(validErr != nullptr) {
        return;
    }

    checkConsistentArea(&graph);
    if(validErr != nullptr) {
        return;
    }

    if(!isSelfTouchingRingFormingHoleValid) {
        checkNoSelfIntersectingRings(&graph);
        if(validErr != nullptr) {
            return;
        }
    }

    checkHolesInShell(g, &graph);
    if(validErr != nullptr) {
        return;
    }

    checkHolesNotNested(g, &graph);
    if(validErr != nullptr) {
        return;
    }

    checkConnectedInteriors(graph);
}

void
IsValidOp::checkValid(const MultiPolygon* g)
{
    auto ngeoms = g->getNumGeometries();
    vector<const Polygon*>polys(ngeoms);

    for(size_t i = 0; i < ngeoms; ++i) {
        const Polygon* p = g->getGeometryN(i);

        checkInvalidCoordinates(p);
        if(validErr != nullptr) {
            return;
        }

        checkClosedRings(p);
        if(validErr != nullptr) {
            return;
        }

        polys[i] = p;
    }

    GeometryGraph graph(0, g);

    checkTooFewPoints(&graph);
    if(validErr != nullptr) {
        return;
    }

    checkConsistentArea(&graph);
    if(validErr != nullptr) {
        return;
    }

    if(!isSelfTouchingRingFormingHoleValid) {
        checkNoSelfIntersectingRings(&graph);
        if(validErr != nullptr) {
            return;
        }
    }

    for(unsigned int i = 0; i < ngeoms; ++i) {
        const Polygon* p = polys[i];
        checkHolesInShell(p, &graph);
        if(validErr != nullptr) {
            return;
        }
    }

    for(unsigned int i = 0; i < ngeoms; ++i) {
        const Polygon* p = polys[i];
        checkHolesNotNested(p, &graph);
        if(validErr != nullptr) {
            return;
        }
    }

    if (ngeoms > 1) {
        checkShellsNotNested(g, &graph);
    }
    if(validErr != nullptr) {
        return;
    }

    checkConnectedInteriors(graph);
}

void
IsValidOp::checkValid(const GeometryCollection* gc)
{
    for(size_t i = 0, ngeoms = gc->getNumGeometries(); i < ngeoms; ++i) {
        const Geometry* g = gc->getGeometryN(i);
        checkValid(g);
        if(validErr != nullptr) {
            return;
        }
    }
}

void
IsValidOp::checkTooFewPoints(GeometryGraph* graph)
{
    if(graph->hasTooFewPoints()) {
        validErr = new TopologyValidationError(
            TopologyValidationError::eTooFewPoints,
            graph->getInvalidPoint());
        return;
    }
}

/**
 * Checks that the arrangement of edges in a polygonal geometry graph
 * forms a consistent area.
 *
 * @param graph
 *
 * @see ConsistentAreaTester
 */
void
IsValidOp::checkConsistentArea(GeometryGraph* graph)
{
    ConsistentAreaTester cat(graph);
    bool isValidArea = cat.isNodeConsistentArea();

    if(!isValidArea) {
        validErr = new TopologyValidationError(
            TopologyValidationError::eSelfIntersection,
            cat.getInvalidPoint());
        return;
    }

    if(cat.hasDuplicateRings()) {
        validErr = new TopologyValidationError(
            TopologyValidationError::eDuplicatedRings,
            cat.getInvalidPoint());
    }
}


/*private*/
void
IsValidOp::checkNoSelfIntersectingRings(GeometryGraph* graph)
{
    vector<Edge*>* edges = graph->getEdges();
    for(unsigned int i = 0; i < edges->size(); ++i) {
        Edge* e = (*edges)[i];
        checkNoSelfIntersectingRing(e->getEdgeIntersectionList());
        if(validErr != nullptr) {
            return;
        }
    }
}

/*private*/
void
IsValidOp::checkNoSelfIntersectingRing(EdgeIntersectionList& eiList)
{
    set<const Coordinate*, CoordinateLessThen>nodeSet;
    bool isFirst = true;
    for(const EdgeIntersection& ei : eiList) {
        if(isFirst) {
            isFirst = false;
            continue;
        }
        if(nodeSet.find(&ei.coord) != nodeSet.end()) {
            validErr = new TopologyValidationError(
                TopologyValidationError::eRingSelfIntersection,
                ei.coord);
            return;
        }
        else {
            nodeSet.insert(&ei.coord);
        }
    }
}

/*private*/
void
IsValidOp::checkHolesInShell(const Polygon* p, GeometryGraph* graph)
{
    auto nholes = p->getNumInteriorRing();
    if (nholes == 0) {
        return;
    }

    const LinearRing* shell = p->getExteriorRing();

    bool isShellEmpty = shell->isEmpty();

    locate::IndexedPointInAreaLocator ipial(*shell);

    for(size_t i = 0; i < nholes; ++i) {
        const LinearRing* hole = p->getInteriorRingN(i);

        if (hole->isEmpty()) continue;

        const Coordinate* holePt = findPtNotNode(hole->getCoordinatesRO(), shell, graph);
        /*
         * If no non-node hole vertex can be found, the hole must
         * split the polygon into disconnected interiors.
         * This will be caught by a subsequent check.
         */
        if (holePt == nullptr) return;
        bool outside = isShellEmpty || (Location::EXTERIOR == ipial.locate(holePt));
        if (outside) {
            validErr = new TopologyValidationError(
                TopologyValidationError::eHoleOutsideShell, *holePt);
            return;
        }
    }

}

/*private*/
void
IsValidOp::checkHolesNotNested(const Polygon* p, GeometryGraph* graph)
{
    //SimpleNestedRingTester nestedTester(graph);
    //SweeplineNestedRingTester nestedTester(graph);
    //QuadtreeNestedRingTester nestedTester(graph);
    auto nholes = p->getNumInteriorRing();

    IndexedNestedRingTester nestedTester(graph, nholes);
    for (size_t i = 0; i < nholes; ++i) {
        const LinearRing* innerHole = p->getInteriorRingN(i);

        //empty holes always pass
        if (innerHole->isEmpty()) {
            continue;
        }

        nestedTester.add(innerHole);
    }

    bool isNonNested = nestedTester.isNonNested();
    if (!isNonNested) {
        validErr = new TopologyValidationError(
            TopologyValidationError::eNestedHoles,
            *(nestedTester.getNestedPoint()));
    }
}

/*private*/
void
IsValidOp::checkShellsNotNested(const MultiPolygon* mp, GeometryGraph* graph)
{
    auto ngeoms = mp->getNumGeometries();

    IndexedNestedShellTester tester(*graph, ngeoms);

    for (size_t i = 0; i < ngeoms; ++i) {
        tester.add(*mp->getGeometryN(i));
    }

    if (!tester.isNonNested()) {
        validErr = new TopologyValidationError(TopologyValidationError::eNestedShells,
                *tester.getNestedPoint());
    }

}


/*private*/
void
IsValidOp::checkConnectedInteriors(GeometryGraph& graph)
{
    ConnectedInteriorTester cit(graph);
    if(!cit.isInteriorsConnected()) {
        validErr = new TopologyValidationError(
            TopologyValidationError::eDisconnectedInterior,
            cit.getCoordinate());
    }
}


/*private*/
void
IsValidOp::checkInvalidCoordinates(const CoordinateSequence* cs)
{
    auto size = cs->size();
    for(size_t i = 0; i < size; ++i) {
        if(! isValid(cs->getAt(i))) {
            validErr = new TopologyValidationError(
                TopologyValidationError::eInvalidCoordinate,
                cs->getAt(i));
            return;

        }
    }
}

/*private*/
void
IsValidOp::checkInvalidCoordinates(const Polygon* poly)
{
    checkInvalidCoordinates(poly->getExteriorRing()->getCoordinatesRO());
    if(validErr != nullptr) {
        return;
    }

    auto nholes = poly->getNumInteriorRing();
    for(size_t i = 0; i < nholes; ++i) {
        checkInvalidCoordinates(
            poly->getInteriorRingN(i)->getCoordinatesRO()
        );
        if(validErr != nullptr) {
            return;
        }
    }
}

/*private*/
void
IsValidOp::checkClosedRings(const Polygon* poly)
{
    const LinearRing* lr = poly->getExteriorRing();
    checkClosedRing(lr);
    if(validErr) {
        return;
    }

    auto nholes = poly->getNumInteriorRing();
    for(size_t i = 0; i < nholes; ++i) {
        lr = (const LinearRing*)poly->getInteriorRingN(i);
        checkClosedRing(lr);
        if(validErr) {
            return;
        }
    }
}

/*private*/
void
IsValidOp::checkClosedRing(const LinearRing* ring)
{
    if(! ring->isClosed() && ! ring->isEmpty()) {
        validErr = new TopologyValidationError(
            TopologyValidationError::eRingNotClosed,
            ring->getCoordinateN(0));
    }
}

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos

