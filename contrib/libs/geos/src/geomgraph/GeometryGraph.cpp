/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geomgraph/GeometryGraph.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/algorithm/Orientation.h>
#include <geos/algorithm/BoundaryNodeRule.h>

#include <geos/util/UnsupportedOperationException.h>
#include <geos/util.h>

#include <geos/geomgraph/GeometryGraph.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/Label.h>
#include <geos/geom/Position.h>

#include <geos/geomgraph/index/SimpleMCSweepLineIntersector.h>
#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/geomgraph/index/EdgeSetIntersector.h>

#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Location.h>
#include <geos/geom/Point.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/util/Interrupt.h>

#include <geos/operation/valid/RepeatedPointRemover.h>

#include <geos/inline.h>

#include <vector>
#include <memory> // unique_ptr
#include <cassert>
#include <typeinfo>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifndef GEOS_INLINE
# include "geos/geomgraph/GeometryGraph.inl"
#endif

using namespace std;
using namespace geos::geomgraph::index;
using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

/*
 * This method implements the Boundary Determination Rule
 * for determining whether
 * a component (node or edge) that appears multiple times in elements
 * of a MultiGeometry is in the boundary or the interior of the Geometry
 *
 * The SFS uses the "Mod-2 Rule", which this function implements
 *
 * An alternative (and possibly more intuitive) rule would be
 * the "At Most One Rule":
 *    isInBoundary = (componentCount == 1)
 */
bool
GeometryGraph::isInBoundary(int boundaryCount)
{
    // the "Mod-2 Rule"
    return boundaryCount % 2 == 1;
}

Location
GeometryGraph::determineBoundary(int boundaryCount)
{
    return isInBoundary(boundaryCount) ? Location::BOUNDARY : Location::INTERIOR;
}


EdgeSetIntersector*
GeometryGraph::createEdgeSetIntersector()
{
    // various options for computing intersections, from slowest to fastest

    //private EdgeSetIntersector esi = new SimpleEdgeSetIntersector();
    //private EdgeSetIntersector esi = new MonotoneChainIntersector();
    //private EdgeSetIntersector esi = new NonReversingChainIntersector();
    //private EdgeSetIntersector esi = new SimpleSweepLineIntersector();
    //private EdgeSetIntersector esi = new MCSweepLineIntersector();

    //return new SimpleEdgeSetIntersector();
    return new SimpleMCSweepLineIntersector();
}

/*public*/
vector<Node*>*
GeometryGraph::getBoundaryNodes()
{
    if(! boundaryNodes.get()) {
        boundaryNodes.reset(new vector<Node*>());
        getBoundaryNodes(*(boundaryNodes.get()));
    }
    return boundaryNodes.get();
}

/*public*/
CoordinateSequence*
GeometryGraph::getBoundaryPoints()
{

    if(! boundaryPoints.get()) {
        // Collection will be destroied by GeometryGraph dtor
        vector<Node*>* coll = getBoundaryNodes();
        boundaryPoints.reset(new CoordinateArraySequence(coll->size()));
        size_t i = 0;
        for(vector<Node*>::iterator it = coll->begin(), endIt = coll->end();
                it != endIt; ++it) {
            Node* node = *it;
            boundaryPoints->setAt(node->getCoordinate(), i++);
        }
    }

    // We keep ownership of this, will be destroyed by destructor
    return boundaryPoints.get();
}

Edge*
GeometryGraph::findEdge(const LineString* line) const
{
    return lineEdgeMap.find(line)->second;
}

void
GeometryGraph::computeSplitEdges(vector<Edge*>* edgelist)
{
#if GEOS_DEBUG
    cerr << "[" << this << "] GeometryGraph::computeSplitEdges() scanning " << edges->size() << " local and " <<
         edgelist->size() << " provided edges" << endl;
#endif
    for(vector<Edge*>::iterator i = edges->begin(), endIt = edges->end();
            i != endIt; ++i) {
        Edge* e = *i;
#if GEOS_DEBUG
        cerr << "   " << e->print() << " adding split edges from arg" << endl;
#endif
        e->eiList.addSplitEdges(edgelist);
    }
#if GEOS_DEBUG
    cerr << "[" << this << "] GeometryGraph::computeSplitEdges() completed " << endl;
#endif
}

void
GeometryGraph::add(const Geometry* g)
//throw (UnsupportedOperationException *)
{
    if(g->isEmpty()) {
        return;
    }

    // check if this Geometry should obey the Boundary Determination Rule
    // all collections except MultiPolygons obey the rule
    if(dynamic_cast<const MultiPolygon*>(g)) {
        useBoundaryDeterminationRule = false;
    }


    if(const Polygon* x1 = dynamic_cast<const Polygon*>(g)) {
        addPolygon(x1);
    }

    // LineString also handles LinearRings
    else if(const LineString* x2 = dynamic_cast<const LineString*>(g)) {
        addLineString(x2);
    }

    else if(const Point* x3 = dynamic_cast<const Point*>(g)) {
        addPoint(x3);
    }

    else if(const GeometryCollection* x4 =
                dynamic_cast<const GeometryCollection*>(g)) {
        addCollection(x4);
    }

    else {
        string out = typeid(*g).name();
        throw util::UnsupportedOperationException("GeometryGraph::add(Geometry *): unknown geometry type: " + out);
    }
}

void
GeometryGraph::addCollection(const GeometryCollection* gc)
{
    for(size_t i = 0, n = gc->getNumGeometries(); i < n; ++i) {
        const Geometry* g = gc->getGeometryN(i);
        add(g);
    }
}

/*
 * Add a Point to the graph.
 */
void
GeometryGraph::addPoint(const Point* p)
{
    const Coordinate& coord = *(p->getCoordinate());
    insertPoint(argIndex, coord, Location::INTERIOR);
}

/*
 * The left and right topological location arguments assume that the ring
 * is oriented CW.
 * If the ring is in the opposite orientation,
 * the left and right locations must be interchanged.
 */
void
GeometryGraph::addPolygonRing(const LinearRing* lr, Location cwLeft, Location cwRight)
// throw IllegalArgumentException (see below)
{
    // skip empty component (see bug #234)
    if(lr->isEmpty()) {
        return;
    }

    const CoordinateSequence* lrcl = lr->getCoordinatesRO();

    auto coord = geos::operation::valid::RepeatedPointRemover::removeRepeatedPoints(lrcl);
    if(coord->getSize() < 4) {
        hasTooFewPointsVar = true;
        invalidPoint = coord->getAt(0); // its now a Coordinate
        return;
    }
    Location left = cwLeft;
    Location right = cwRight;

    /*
     * the isCCW call might throw an
     * IllegalArgumentException if degenerate ring does
     * not contain 3 distinct points.
     */
    if(Orientation::isCCW(coord.get())) {
        left = cwRight;
        right = cwLeft;
    }

    auto coordRaw = coord.release();
    Edge* e = new Edge(coordRaw, Label(argIndex, Location::BOUNDARY, left, right));
    lineEdgeMap[lr] = e;
    insertEdge(e);
    insertPoint(argIndex, coordRaw->getAt(0), Location::BOUNDARY);
}

void
GeometryGraph::addPolygon(const Polygon* p)
{
    const LinearRing* lr = p->getExteriorRing();

    addPolygonRing(lr, Location::EXTERIOR, Location::INTERIOR);
    for(size_t i = 0, n = p->getNumInteriorRing(); i < n; ++i) {
        // Holes are topologically labelled opposite to the shell, since
        // the interior of the polygon lies on their opposite side
        // (on the left, if the hole is oriented CW)
        lr = p->getInteriorRingN(i);
        addPolygonRing(lr, Location::INTERIOR, Location::EXTERIOR);
    }
}

void
GeometryGraph::addLineString(const LineString* line)
{
    auto coord = operation::valid::RepeatedPointRemover::removeRepeatedPoints(line->getCoordinatesRO());
    if(coord->getSize() < 2) {
        hasTooFewPointsVar = true;
        invalidPoint = coord->getAt(0);
        return;
    }

    auto coordRaw = coord.release();
    Edge* e = new Edge(coordRaw, Label(argIndex, Location::INTERIOR));
    lineEdgeMap[line] = e;
    insertEdge(e);

    /*
     * Add the boundary points of the LineString, if any.
     * Even if the LineString is closed, add both points as if they
     * were endpoints.
     * This allows for the case that the node already exists and is
     * a boundary point.
     */
    assert(coordRaw->size() >= 2); // found LineString with single point
    insertBoundaryPoint(argIndex, coordRaw->getAt(0));
    insertBoundaryPoint(argIndex, coordRaw->getAt(coordRaw->getSize() - 1));
}

/*
 * Add an Edge computed externally.  The label on the Edge is assumed
 * to be correct.
 */
void
GeometryGraph::addEdge(Edge* e)
{
    insertEdge(e);
    const CoordinateSequence* coord = e->getCoordinates();
    // insert the endpoint as a node, to mark that it is on the boundary
    insertPoint(argIndex, coord->getAt(0), Location::BOUNDARY);
    insertPoint(argIndex, coord->getAt(coord->getSize() - 1), Location::BOUNDARY);
}

/*
 * Add a point computed externally.  The point is assumed to be a
 * Point Geometry part, which has a location of INTERIOR.
 */
void
GeometryGraph::addPoint(Coordinate& pt)
{
    insertPoint(argIndex, pt, Location::INTERIOR);
}

template <class T, class C>
void
collect_intersecting_edges(const Envelope* env, T start, T end, C& to)
{
    for(T i = start; i != end; ++i) {
        Edge* e = *i;
        if(e->getEnvelope()->intersects(env)) {
            to.push_back(e);
        }
    }
}

/*public*/
std::unique_ptr<SegmentIntersector>
GeometryGraph::computeSelfNodes(LineIntersector& li,
                                bool computeRingSelfNodes, const Envelope* env)
{
    return computeSelfNodes(li, computeRingSelfNodes, false, env);
}

std::unique_ptr<SegmentIntersector>
GeometryGraph::computeSelfNodes(LineIntersector& li,
                                bool computeRingSelfNodes, bool isDoneIfProperInt, const Envelope* env)
{
    auto si = detail::make_unique<SegmentIntersector>(&li, true, false);
    si->setIsDoneIfProperInt(isDoneIfProperInt);
    unique_ptr<EdgeSetIntersector> esi(createEdgeSetIntersector());

    typedef vector<Edge*> EC;
    EC* se = edges;
    EC self_edges_copy;

    if(env && ! env->covers(parentGeom->getEnvelopeInternal())) {
        collect_intersecting_edges(env, se->begin(), se->end(), self_edges_copy);
        //cerr << "(computeSelfNodes) Self edges reduced from " << se->size() << " to " << self_edges_copy.size() << endl;
        se = &self_edges_copy;
    }

    bool isRings = dynamic_cast<const LinearRing*>(parentGeom)
                   || dynamic_cast<const Polygon*>(parentGeom)
                   || dynamic_cast<const MultiPolygon*>(parentGeom);

    bool computeAllSegments = computeRingSelfNodes || ! isRings;

    esi->computeIntersections(se, si.get(), computeAllSegments);

#if GEOS_DEBUG
    cerr << "SegmentIntersector # tests = " << si->numTests << endl;
#endif // GEOS_DEBUG

    addSelfIntersectionNodes(argIndex);
    return si;
}

std::unique_ptr<SegmentIntersector>
GeometryGraph::computeEdgeIntersections(GeometryGraph* g,
                                        LineIntersector* li, bool includeProper, const Envelope* env)
{
#if GEOS_DEBUG
    cerr << "GeometryGraph::computeEdgeIntersections call" << endl;
#endif
    unique_ptr<SegmentIntersector> si(new SegmentIntersector(li, includeProper, true));

    si->setBoundaryNodes(getBoundaryNodes(), g->getBoundaryNodes());
    unique_ptr<EdgeSetIntersector> esi(createEdgeSetIntersector());

    typedef vector<Edge*> EC;

    EC self_edges_copy;
    EC other_edges_copy;

    EC* se = edges;
    EC* oe = g->edges;
    if(env && ! env->covers(parentGeom->getEnvelopeInternal())) {
        collect_intersecting_edges(env, se->begin(), se->end(), self_edges_copy);
        //cerr << "Self edges reduced from " << se->size() << " to " << self_edges_copy.size() << endl;
        se = &self_edges_copy;
    }
    if(env && ! env->covers(g->parentGeom->getEnvelopeInternal())) {
        collect_intersecting_edges(env, oe->begin(), oe->end(), other_edges_copy);
        //cerr << "Other edges reduced from " << oe->size() << " to " << other_edges_copy.size() << endl;
        oe = &other_edges_copy;
    }
    esi->computeIntersections(se, oe, si.get());
#if GEOS_DEBUG
    cerr << "GeometryGraph::computeEdgeIntersections returns" << endl;
#endif
    return si;
}

void
GeometryGraph::insertPoint(int p_argIndex, const Coordinate& coord,
                           geom::Location onLocation)
{
#if GEOS_DEBUG > 1
    cerr << "GeometryGraph::insertPoint(" << coord.toString() << " called" << endl;
#endif
    Node* n = nodes->addNode(coord);
    Label& lbl = n->getLabel();
    if(lbl.isNull()) {
        n->setLabel(p_argIndex, onLocation);
    }
    else {
        lbl.setLocation(p_argIndex, onLocation);
    }
}

/*
 * Adds points using the mod-2 rule of SFS.  This is used to add the boundary
 * points of dim-1 geometries (Curves/MultiCurves).  According to the SFS,
 * an endpoint of a Curve is on the boundary
 * iff if it is in the boundaries of an odd number of Geometries
 */
void
GeometryGraph::insertBoundaryPoint(int p_argIndex, const Coordinate& coord)
{
    Node* n = nodes->addNode(coord);
    // nodes always have labels
    Label& lbl = n->getLabel();

    // the new point to insert is on a boundary
    int boundaryCount = 1;

    // determine the current location for the point (if any)
    Location loc = lbl.getLocation(p_argIndex, Position::ON);
    if(loc == Location::BOUNDARY) {
        boundaryCount++;
    }

    // determine the boundary status of the point according to the
    // Boundary Determination Rule
    Location newLoc = determineBoundary(boundaryNodeRule, boundaryCount);
    lbl.setLocation(p_argIndex, newLoc);
}

/*private*/
void
GeometryGraph::addSelfIntersectionNodes(int p_argIndex)
{
    for(vector<Edge*>::iterator i = edges->begin(), endIt = edges->end();
            i != endIt; ++i) {
        Edge* e = *i;
        Location eLoc = e->getLabel().getLocation(p_argIndex);
        EdgeIntersectionList& eiL = e->eiList;
        for(const EdgeIntersection& ei : eiL) {
            addSelfIntersectionNode(p_argIndex, ei.coord, eLoc);
            GEOS_CHECK_FOR_INTERRUPTS();
        }
    }
}

/*private*/
void
GeometryGraph::addSelfIntersectionNode(int p_argIndex,
                                       const Coordinate& coord, Location loc)
{
    // if this node is already a boundary node, don't change it
    if(isBoundaryNode(p_argIndex, coord)) {
        return;
    }
    if(loc == Location::BOUNDARY && useBoundaryDeterminationRule) {
        insertBoundaryPoint(p_argIndex, coord);
    }
    else {
        insertPoint(p_argIndex, coord, loc);
    }
}

vector<Edge*>*
GeometryGraph::getEdges()
{
    return edges;
}

bool
GeometryGraph::hasTooFewPoints()
{
    return hasTooFewPointsVar;
}

const Coordinate&
GeometryGraph::getInvalidPoint()
{
    return invalidPoint;
}

GeometryGraph::GeometryGraph(int newArgIndex,
                             const geom::Geometry* newParentGeom)
    :
    PlanarGraph(),
    parentGeom(newParentGeom),
    useBoundaryDeterminationRule(true),
    boundaryNodeRule(algorithm::BoundaryNodeRule::getBoundaryOGCSFS()),
    argIndex(newArgIndex),
    hasTooFewPointsVar(false)
{
    if(parentGeom != nullptr) {
        add(parentGeom);
    }
}

GeometryGraph::GeometryGraph(int newArgIndex,
                             const geom::Geometry* newParentGeom,
                             const algorithm::BoundaryNodeRule& bnr)
    :
    PlanarGraph(),
    parentGeom(newParentGeom),
    useBoundaryDeterminationRule(true),
    boundaryNodeRule(bnr),
    argIndex(newArgIndex),
    hasTooFewPointsVar(false)
{
    if(parentGeom != nullptr) {
        add(parentGeom);
    }
}

GeometryGraph::GeometryGraph()
    :
    PlanarGraph(),
    parentGeom(nullptr),
    useBoundaryDeterminationRule(true),
    boundaryNodeRule(algorithm::BoundaryNodeRule::getBoundaryOGCSFS()),
    argIndex(-1),
    hasTooFewPointsVar(false)
{
}


/* public static */
Location
GeometryGraph::determineBoundary(
    const algorithm::BoundaryNodeRule& boundaryNodeRule,
    int boundaryCount)
{
    return boundaryNodeRule.isInBoundary(boundaryCount)
           ? Location::BOUNDARY : Location::INTERIOR;
}

} // namespace geos.geomgraph
} // namespace geos

