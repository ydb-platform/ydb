/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009      Sandro Santilli <strk@kbt.io>
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
 * Last port: operation/IsSimpleOp.java rev. 1.22 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/IsSimpleOp.h>
//#include <geos/operation/EndpointInfo.h>
#include <geos/algorithm/BoundaryNodeRule.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/geomgraph/GeometryGraph.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/EdgeIntersection.h>
#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Point.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/util/LinearComponentExtracter.h>

#include <set>
#include <cassert>

using namespace std;
using namespace geos::algorithm;
using namespace geos::geomgraph;
using namespace geos::geomgraph::index;
using namespace geos::geom;
using namespace geos::geom::util;

namespace geos {
namespace operation { // geos.operation

// This is supposedly a private of IsSimpleOp...
class EndpointInfo {
public:

    Coordinate pt;

    bool isClosed;

    int degree;

    EndpointInfo(const geom::Coordinate& newPt);

    const Coordinate&
    getCoordinate() const
    {
        return pt;
    }

    void
    addEndpoint(bool newIsClosed)
    {
        degree++;
        isClosed |= newIsClosed;
    }
};

EndpointInfo::EndpointInfo(const Coordinate& newPt)
{
    pt = newPt;
    isClosed = false;
    degree = 0;
}

// -----------------------------------------------------



/*public*/
IsSimpleOp::IsSimpleOp()
    :
    isClosedEndpointsInInterior(true),
    geom(nullptr),
    nonSimpleLocation()
{}

/*public*/
IsSimpleOp::IsSimpleOp(const Geometry& g)
    :
    isClosedEndpointsInInterior(true),
    geom(&g),
    nonSimpleLocation()
{}

/*public*/
IsSimpleOp::IsSimpleOp(const Geometry& g,
                       const BoundaryNodeRule& boundaryNodeRule)
    :
    isClosedEndpointsInInterior(! boundaryNodeRule.isInBoundary(2)),
    geom(&g),
    nonSimpleLocation()
{}

/*public*/
bool
IsSimpleOp::isSimple()
{
    nonSimpleLocation.reset();
    return computeSimple(geom);
}


/*public*/
bool
IsSimpleOp::isSimple(const LineString* p_geom)
{
    return isSimpleLinearGeometry(p_geom);
}

/*public*/
bool
IsSimpleOp::isSimple(const MultiLineString* p_geom)
{
    return isSimpleLinearGeometry(p_geom);
}

/*public*/
bool
IsSimpleOp::isSimple(const MultiPoint* mp)
{
    return isSimpleMultiPoint(*mp);
}

/*private*/
bool
IsSimpleOp::isSimpleMultiPoint(const MultiPoint& mp)
{
    if(mp.isEmpty()) {
        return true;
    }
    set<const Coordinate*, CoordinateLessThen> points;

    for(std::size_t i = 0, n = mp.getNumGeometries(); i < n; ++i) {
        const Point* pt = mp.getGeometryN(i);
        assert(pt);
        const Coordinate* p = pt->getCoordinate();
        if(points.find(p) != points.end()) {
            nonSimpleLocation.reset(new Coordinate(*p));
            return false;
        }
        points.insert(p);
    }
    return true;
}

bool
IsSimpleOp::isSimpleLinearGeometry(const Geometry* p_geom)
{
    if(p_geom->isEmpty()) {
        return true;
    }
    GeometryGraph graph(0, p_geom);
    LineIntersector li;
    std::unique_ptr<SegmentIntersector> si(graph.computeSelfNodes(&li, true));

    // if no self-intersection, must be simple
    if(!si->hasIntersection()) {
        return true;
    }

    if(si->hasProperIntersection()) {
        nonSimpleLocation.reset(
            new Coordinate(si->getProperIntersectionPoint())
        );
        return false;
    }

    if(hasNonEndpointIntersection(graph)) {
        return false;
    }

    if(isClosedEndpointsInInterior) {
        if(hasClosedEndpointIntersection(graph)) {
            return false;
        }
    }

    return true;
}

/*private*/
bool
IsSimpleOp::hasNonEndpointIntersection(GeometryGraph& graph)
{
    vector<Edge*>* edges = graph.getEdges();
    for(Edge* e: *edges) {
        auto maxSegmentIndex = e->getMaximumSegmentIndex();
        EdgeIntersectionList& eiL = e->getEdgeIntersectionList();
        for(const EdgeIntersection& ei : eiL) {
            if(!ei.isEndPoint(maxSegmentIndex)) {
                nonSimpleLocation.reset(
                    new Coordinate(ei.getCoordinate())
                );
                return true;
            }
        }
    }
    return false;
}

/*private*/
bool
IsSimpleOp::computeSimple(const geom::Geometry* g)
{
    nonSimpleLocation.reset();

    if(dynamic_cast<const LineString*>(g)) {
        return isSimpleLinearGeometry(g);
    }

    if(dynamic_cast<const LinearRing*>(g)) {
        return isSimpleLinearGeometry(g);
    }

    if(dynamic_cast<const MultiLineString*>(g)) {
        return isSimpleLinearGeometry(g);
    }

    if(dynamic_cast<const Polygon*>(g)) {
        return isSimplePolygonal(g);
    }

    const MultiPoint* mp = dynamic_cast<const MultiPoint*>(g);
    if(mp) {
        return isSimpleMultiPoint(*mp);
    }

    // This must be after MultiPoint test, as MultiPoint can
    // cast cleanly into GeometryCollection
    const GeometryCollection* gc = dynamic_cast<const GeometryCollection*>(g);
    if(gc) {
        return isSimpleGeometryCollection(gc);
    }

    // all other geometry types are simple by definition
    return true;
}

/*private*/
bool
IsSimpleOp::isSimpleGeometryCollection(const geom::GeometryCollection* col)
{
    for(const auto& g : *col) {
        if(!computeSimple(g.get())) {
            return false;
        }
    }
    return true;
}

/*private*/
bool
IsSimpleOp::isSimplePolygonal(const geom::Geometry* g)
{

    LineString::ConstVect rings;
    LinearComponentExtracter::getLines(*g, rings);
    for(const geom::LineString* ring : rings) {
        if(!isSimpleLinearGeometry(ring)) {
            return false;
        }
    }
    return true;
}

/*private*/
bool
IsSimpleOp::hasClosedEndpointIntersection(GeometryGraph& graph)
{
    map<const Coordinate*, EndpointInfo*, CoordinateLessThen> endPoints;
    vector<Edge*>* edges = graph.getEdges();
    for(Edge* e: *edges) {
        //int maxSegmentIndex=e->getMaximumSegmentIndex();
        bool isClosed = e->isClosed();
        const Coordinate* p0 = &e->getCoordinate(0);
        addEndpoint(endPoints, p0, isClosed);
        const Coordinate* p1 = &e->getCoordinate(e->getNumPoints() - 1);
        addEndpoint(endPoints, p1, isClosed);
    }

    map<const Coordinate*, EndpointInfo*, CoordinateLessThen>::iterator it = endPoints.begin();
    for(; it != endPoints.end(); ++it) {
        EndpointInfo* eiInfo = it->second;
        if(eiInfo->isClosed && eiInfo->degree != 2) {

            nonSimpleLocation.reset(
                new Coordinate(eiInfo->getCoordinate())
            );

            it = endPoints.begin();
            for(; it != endPoints.end(); ++it) {
                EndpointInfo* ep = it->second;
                delete ep;
            }
            return true;
        }
    }

    it = endPoints.begin();
    for(; it != endPoints.end(); ++it) {
        EndpointInfo* ep = it->second;
        delete ep;
    }
    return false;
}

/*private*/
void
IsSimpleOp::addEndpoint(
    map<const Coordinate*, EndpointInfo*, CoordinateLessThen>& endPoints,
    const Coordinate* p, bool isClosed)
{
    map<const Coordinate*, EndpointInfo*, CoordinateLessThen>::iterator it = endPoints.find(p);
    EndpointInfo* eiInfo;
    if(it == endPoints.end()) {
        eiInfo = nullptr;
    }
    else {
        eiInfo = it->second;
    }
    if(eiInfo == nullptr) {
        eiInfo = new EndpointInfo(*p);
        endPoints[p] = eiInfo;
    }
    eiInfo->addEndpoint(isClosed);
}

} // namespace geos::operation
} // namespace geos

