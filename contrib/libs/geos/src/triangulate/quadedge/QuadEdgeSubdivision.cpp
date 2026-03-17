/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 * Copyright (C) 2019 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/quadedge/QuadEdgeSubdivision.java r524
 *
 **********************************************************************/
#include <geos/triangulate/quadedge/QuadEdgeSubdivision.h>

#include <algorithm>
#include <vector>
#include <set>
#include <iostream>

#include <geos/geom/Polygon.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/CoordinateList.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/util.h>
#include <geos/triangulate/quadedge/QuadEdge.h>
#include <geos/triangulate/quadedge/QuadEdgeLocator.h>
#include <geos/triangulate/quadedge/LastFoundQuadEdgeLocator.h>
#include <geos/triangulate/quadedge/LocateFailureException.h>
#include <geos/triangulate/quadedge/TriangleVisitor.h>
#include <geos/geom/Triangle.h>


using namespace geos::geom;
using namespace std;

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

void
QuadEdgeSubdivision::getTriangleEdges(const QuadEdge& startQE,
                                      const QuadEdge* triEdge[3])
{
    triEdge[0] = &startQE;
    triEdge[1] = &triEdge[0]->lNext();
    triEdge[2] = &triEdge[1]->lNext();
    if(&triEdge[2]->lNext() != triEdge[0]) {
        throw util::IllegalArgumentException("Edges do not form a triangle");
    }
}

QuadEdgeSubdivision::QuadEdgeSubdivision(const geom::Envelope& env, double p_tolerance) :
    tolerance(p_tolerance),
    locator(new LastFoundQuadEdgeLocator(this)),
    visit_state_clean(true)
{
    edgeCoincidenceTolerance = tolerance / EDGE_COINCIDENCE_TOL_FACTOR;
    createFrame(env);
    initSubdiv();
}

void
QuadEdgeSubdivision::createFrame(const geom::Envelope& env)
{
    double deltaX = env.getWidth();
    double deltaY = env.getHeight();
    double offset = std::max(deltaX, deltaY) * FRAME_SIZE_FACTOR;

    frameVertex[0] = Vertex((env.getMaxX() + env.getMinX()) / 2.0,
                            env.getMaxY() + offset);
    frameVertex[1] = Vertex(env.getMinX() - offset, env.getMinY() - offset);
    frameVertex[2] = Vertex(env.getMaxX() + offset, env.getMinY() - offset);

    frameEnv = Envelope(frameVertex[0].getCoordinate(),
                        frameVertex[1].getCoordinate());
    frameEnv.expandToInclude(frameVertex[2].getCoordinate());
}
void
QuadEdgeSubdivision::initSubdiv()
{
    assert(quadEdges.empty());

    // build initial subdivision from frame
    startingEdges[0] = QuadEdge::makeEdge(frameVertex[0], frameVertex[1], quadEdges);
    startingEdges[1] = QuadEdge::makeEdge(frameVertex[1], frameVertex[2], quadEdges);
    QuadEdge::splice(startingEdges[0]->sym(), *startingEdges[1]);

    startingEdges[2] = QuadEdge::makeEdge(frameVertex[2], frameVertex[0], quadEdges);
    QuadEdge::splice(startingEdges[1]->sym(), *startingEdges[2]);
    QuadEdge::splice(startingEdges[2]->sym(), *startingEdges[0]);
}

QuadEdge&
QuadEdgeSubdivision::makeEdge(const Vertex& o, const Vertex& d)
{
    QuadEdge* e = QuadEdge::makeEdge(o, d, quadEdges);
    return *e;
}

QuadEdge&
QuadEdgeSubdivision::connect(QuadEdge& a, QuadEdge& b)
{
    QuadEdge* e = QuadEdge::connect(a, b, quadEdges);
    return *e;
}

void
QuadEdgeSubdivision::remove(QuadEdge& e)
{
    QuadEdge::splice(e, e.oPrev());
    QuadEdge::splice(e.sym(), e.sym().oPrev());

    // because QuadEdge pointers must be stable, do not remove edge from quadedges container
    // This is fine since they are detached from the subdivision

    e.remove();
}

QuadEdge*
QuadEdgeSubdivision::locateFromEdge(const Vertex& v,
                                    const QuadEdge& startEdge) const
{
    ::geos::ignore_unused_variable_warning(startEdge);

    size_t iter = 0;
    auto maxIter = quadEdges.size();

    QuadEdge* e = startingEdges[0];

    for(;;) {
        ++iter;
        /*
         * So far it has always been the case that failure to locate indicates an
         * invalid subdivision. So just fail completely. (An alternative would be
         * to perform an exhaustive search for the containing triangle, but this
         * would mask errors in the subdivision topology)
         *
         * This can also happen if two vertices are located very close together,
         * since the orientation predicates may experience precision failures.
         */
        if(iter > maxIter) {
            throw LocateFailureException("Could not locate vertex.");
        }

        if((v.equals(e->orig())) || (v.equals(e->dest()))) {
            break;
        }
        else if(v.rightOf(*e)) {
            e = &e->sym();
        }
        else if(!v.rightOf(e->oNext())) {
            e = &e->oNext();
        }
        else if(!v.rightOf(e->dPrev())) {
            e = &e->dPrev();
        }
        else {
            // on edge or in triangle containing edge
            break;
        }
    }
    return e;
}

QuadEdge*
QuadEdgeSubdivision::locate(const Coordinate& p0, const Coordinate& p1)
{
    // find an edge containing one of the points
    QuadEdge* e = locator->locate(Vertex(p0));
    if(e == nullptr) {
        return nullptr;
    }

    // normalize so that p0 is origin of base edge
    QuadEdge* base = e;
    if(e->dest().getCoordinate().equals2D(p0)) {
        base = &e->sym();
    }
    // check all edges around origin of base edge
    QuadEdge* locEdge = base;
    do {
        if(locEdge->dest().getCoordinate().equals2D(p1)) {
            return locEdge;
        }
        locEdge = &locEdge->oNext();
    }
    while(locEdge != base);
    return nullptr;
}

QuadEdge&
QuadEdgeSubdivision::insertSite(const Vertex& v)
{
    QuadEdge* e = locate(v);

    if((v.equals(e->orig(), tolerance)) || (v.equals(e->dest(), tolerance))) {
        return *e; // point already in subdivision.
    }

    // Connect the new point to the vertices of the containing
    // triangle (or quadrilateral, if the new point fell on an
    // existing edge.)
    QuadEdge* base = &makeEdge(e->orig(), v);
    QuadEdge::splice(*base, *e);
    QuadEdge* startEdge = base;
    do {
        base = &connect(*e, base->sym());
        e = &base->oPrev();
    }
    while(&e->lNext() != startEdge);

    return *startEdge;
}

bool
QuadEdgeSubdivision::isFrameEdge(const QuadEdge& e) const
{
    if(isFrameVertex(e.orig()) || isFrameVertex(e.dest())) {
        return true;
    }
    return false;
}

bool
QuadEdgeSubdivision::isFrameBorderEdge(const QuadEdge& e) const
{
    // check other vertex of triangle to left of edge
    Vertex vLeftTriOther = e.lNext().dest();
    if(isFrameVertex(vLeftTriOther)) {
        return true;
    }
    // check other vertex of triangle to right of edge
    Vertex vRightTriOther = e.sym().lNext().dest();
    if(isFrameVertex(vRightTriOther)) {
        return true;
    }

    return false;
}

bool
QuadEdgeSubdivision::isFrameVertex(const Vertex& v) const
{
    if(v.equals(frameVertex[0])) {
        return true;
    }
    if(v.equals(frameVertex[1])) {
        return true;
    }
    if(v.equals(frameVertex[2])) {
        return true;
    }
    return false;
}

bool
QuadEdgeSubdivision::isOnEdge(const QuadEdge& e, const Coordinate& p) const
{
    geom::LineSegment seg;
    seg.setCoordinates(e.orig().getCoordinate(), e.dest().getCoordinate());
    double dist = seg.distance(p);
    // heuristic (hack?)
    return dist < edgeCoincidenceTolerance;
}

bool
QuadEdgeSubdivision::isVertexOfEdge(const QuadEdge& e, const Vertex& v) const
{
    if((v.equals(e.orig(), tolerance)) || (v.equals(e.dest(), tolerance))) {
        return true;
    }
    return false;
}

std::unique_ptr<QuadEdgeSubdivision::QuadEdgeList>
QuadEdgeSubdivision::getPrimaryEdges(bool includeFrame)
{
    QuadEdgeList* edges = new QuadEdgeList();
    QuadEdgeStack edgeStack;

    edgeStack.push(startingEdges[0]);

    prepareVisit();

    while(!edgeStack.empty()) {
        QuadEdge* edge = edgeStack.top();
        edgeStack.pop();
        if(!edge->isVisited()) {
            QuadEdge* priQE = (QuadEdge*)&edge->getPrimary();

            if(includeFrame || ! isFrameEdge(*priQE)) {
                edges->push_back(priQE);
            }

            edgeStack.push(&edge->oNext());
            edgeStack.push(&edge->sym().oNext());

            edge->setVisited(true);
            edge->sym().setVisited(true);
        }
    }
    return std::unique_ptr<QuadEdgeList>(edges);
}

QuadEdge**
QuadEdgeSubdivision::fetchTriangleToVisit(QuadEdge* edge,
        QuadEdgeStack& edgeStack, bool includeFrame)
{
    QuadEdge* curr = edge;
    int edgeCount = 0;
    bool isFrame = false;
    do {
        triEdges[edgeCount] = curr;

        if(!includeFrame && isFrameEdge(*curr)) {
            isFrame = true;
        }

        // push sym edges to visit next
        QuadEdge* sym = &curr->sym();
        if (!sym->isVisited()) {
            edgeStack.push(sym);
        }

        // mark this edge as visited
        curr->setVisited(true);

        edgeCount++;
        curr = &curr->lNext();
    }
    while(curr != edge);

    if(!includeFrame && isFrame) {
        return nullptr;
    }
    return triEdges;
}

class
    QuadEdgeSubdivision::TriangleCoordinatesVisitor : public TriangleVisitor {
private:
    QuadEdgeSubdivision::TriList* triCoords;
    CoordinateArraySequenceFactory coordSeqFact;

public:
    TriangleCoordinatesVisitor(QuadEdgeSubdivision::TriList* p_triCoords): triCoords(p_triCoords)
    {
    }

    void
    visit(QuadEdge* triEdges[3]) override
    {
        auto coordSeq = coordSeqFact.create(4, 0);
        for(size_t i = 0; i < 3; i++) {
            Vertex v = triEdges[i]->orig();
            coordSeq->setAt(v.getCoordinate(), i);
        }
        coordSeq->setAt(triEdges[0]->orig().getCoordinate(), 3);
        triCoords->push_back(std::move(coordSeq));
    }
};


class
    QuadEdgeSubdivision::TriangleCircumcentreVisitor : public TriangleVisitor {
public:
    void
    visit(QuadEdge* triEdges[3]) override
    {
        Triangle triangle(triEdges[0]->orig().getCoordinate(),
                          triEdges[1]->orig().getCoordinate(), triEdges[2]->orig().getCoordinate());
        Coordinate cc;

        //TODO: identify heuristic to allow calling faster circumcentre() when possible
        triangle.circumcentreDD(cc);

        Vertex ccVertex(cc);

        for(int i = 0 ; i < 3 ; i++) {
            triEdges[i]->rot().setOrig(ccVertex);
        }
    }
};


void
QuadEdgeSubdivision::getTriangleCoordinates(QuadEdgeSubdivision::TriList* triList, bool includeFrame)
{
    TriangleCoordinatesVisitor visitor(triList);
    visitTriangles(&visitor, includeFrame);
}

void
QuadEdgeSubdivision::prepareVisit() {
    if (!visit_state_clean) {
        for (auto& qe : quadEdges) {
            qe.setVisited(false);
        }
    }

    visit_state_clean = false;
}

void
QuadEdgeSubdivision::visitTriangles(TriangleVisitor* triVisitor, bool includeFrame)
{
    QuadEdgeStack edgeStack;
    edgeStack.push(startingEdges[0]);

    prepareVisit();

    while(!edgeStack.empty()) {
        QuadEdge* edge = edgeStack.top();
        edgeStack.pop();
        if(!edge->isVisited()) {
            QuadEdge** p_triEdges = fetchTriangleToVisit(edge, edgeStack, includeFrame);
            if(p_triEdges != nullptr) {
                triVisitor->visit(p_triEdges);
            }
        }
    }
}

std::unique_ptr<geom::MultiLineString>
QuadEdgeSubdivision::getEdges(const geom::GeometryFactory& geomFact)
{
    std::unique_ptr<QuadEdgeList> p_quadEdges(getPrimaryEdges(false));
    std::vector<std::unique_ptr<Geometry>> edges;
    const CoordinateSequenceFactory* coordSeqFact = geomFact.getCoordinateSequenceFactory();

    edges.reserve(p_quadEdges->size());
    for(const QuadEdge* qe : *p_quadEdges) {
        auto coordSeq = coordSeqFact->create(2);

        coordSeq->setAt(qe->orig().getCoordinate(), 0);
        coordSeq->setAt(qe->dest().getCoordinate(), 1);

        edges.emplace_back(geomFact.createLineString(coordSeq.release()));
    }

    return geomFact.createMultiLineString(std::move(edges));
}

std::unique_ptr<GeometryCollection>
QuadEdgeSubdivision::getTriangles(const GeometryFactory& geomFact)
{
    TriList triPtsList;
    getTriangleCoordinates(&triPtsList, false);
    std::vector<std::unique_ptr<Geometry>> tris;
    tris.reserve(triPtsList.size());

    for(auto& coordSeq : triPtsList) {
        tris.push_back(
                geomFact.createPolygon(geomFact.createLinearRing(std::move(coordSeq))));
    }

    return geomFact.createGeometryCollection(std::move(tris));
}


//Methods for VoronoiDiagram
std::unique_ptr<geom::GeometryCollection>
QuadEdgeSubdivision::getVoronoiDiagram(const geom::GeometryFactory& geomFact)
{
    return geomFact.createGeometryCollection(getVoronoiCellPolygons(geomFact));
}

std::unique_ptr<geom::MultiLineString>
QuadEdgeSubdivision::getVoronoiDiagramEdges(const geom::GeometryFactory& geomFact)
{
    return geomFact.createMultiLineString(getVoronoiCellEdges(geomFact));
}

std::vector<std::unique_ptr<geom::Geometry>>
QuadEdgeSubdivision::getVoronoiCellPolygons(const geom::GeometryFactory& geomFact)
{
    std::vector<std::unique_ptr<geom::Geometry>> cells;
    TriangleCircumcentreVisitor tricircumVisitor;

    visitTriangles(&tricircumVisitor, true);

    std::unique_ptr<QuadEdgeSubdivision::QuadEdgeList> edges = getVertexUniqueEdges(false);

    cells.reserve(edges->size());
    for(const QuadEdge* qe : *edges) {
        cells.push_back(getVoronoiCellPolygon(qe, geomFact));
    }

    return cells;
}

std::vector<std::unique_ptr<geom::Geometry>>
QuadEdgeSubdivision::getVoronoiCellEdges(const geom::GeometryFactory& geomFact)
{
    std::vector<std::unique_ptr<geom::Geometry>> cells;
    TriangleCircumcentreVisitor tricircumVisitor;

    visitTriangles((TriangleVisitor*) &tricircumVisitor, true);

    std::unique_ptr<QuadEdgeSubdivision::QuadEdgeList> edges = getVertexUniqueEdges(false);
    cells.reserve(edges->size());

    for(const QuadEdge* qe : *edges) {
        cells.push_back(getVoronoiCellEdge(qe, geomFact));
    }

    return cells;
}

std::unique_ptr<geom::Geometry>
QuadEdgeSubdivision::getVoronoiCellPolygon(const QuadEdge* qe, const geom::GeometryFactory& geomFact)
{
    std::vector<Coordinate> cellPts;

    const QuadEdge* startQE = qe;
    do {
        const Coordinate& cc = qe->rot().orig().getCoordinate();
        if(cellPts.empty() || cellPts.back() != cc) {  // no duplicates
            cellPts.push_back(cc);
        }
        qe = &qe->oPrev();

    }
    while(qe != startQE);

    // Close the ring
    if (cellPts.front() != cellPts.back()) {
        cellPts.push_back(cellPts.front());
    }
    if (cellPts.size() < 4) {
        cellPts.push_back(cellPts.back());
    }

    auto seq = geomFact.getCoordinateSequenceFactory()->create(std::move(cellPts));
    std::unique_ptr<Geometry> cellPoly = geomFact.createPolygon(geomFact.createLinearRing(std::move(seq)));

    // FIXME why is this returning a pointer to a local variable?
    Vertex v = startQE->orig();
    Coordinate c(0, 0);
    c = v.getCoordinate();
    cellPoly->setUserData(reinterpret_cast<void*>(&c));
    return cellPoly;
}

std::unique_ptr<geom::Geometry>
QuadEdgeSubdivision::getVoronoiCellEdge(const QuadEdge* qe, const geom::GeometryFactory& geomFact)
{
    std::vector<Coordinate> cellPts;

    const QuadEdge* startQE = qe;
    do {
        const Coordinate& cc = qe->rot().orig().getCoordinate();
        if(cellPts.empty() || cellPts.back() != cc) {  // no duplicates
            cellPts.push_back(cc);
        }
        qe = &qe->oPrev();

    }
    while(qe != startQE);

    // Close the ring
    if (cellPts.front() != cellPts.back()) {
        cellPts.push_back(cellPts.front());
    }

    std::unique_ptr<geom::Geometry> cellEdge(
        geomFact.createLineString(new geom::CoordinateArraySequence(std::move(cellPts))));

    // FIXME why is this returning a pointer to a local variable?
    Vertex v = startQE->orig();
    Coordinate c(0, 0);
    c = v.getCoordinate();
    cellEdge->setUserData(reinterpret_cast<void*>(&c));
    return cellEdge;
}

std::unique_ptr<QuadEdgeSubdivision::QuadEdgeList>
QuadEdgeSubdivision::getVertexUniqueEdges(bool includeFrame)
{
    auto edges = detail::make_unique<QuadEdgeList>();
    std::set<Vertex> visitedVertices; // TODO unordered_set of Vertex* ?

    for(auto& quartet : quadEdges) {
        QuadEdge* qe = &quartet.base();
        const Vertex& v = qe->orig();

        if(visitedVertices.find(v) == visitedVertices.end()) {	//if v not found
            visitedVertices.insert(v);

            if(includeFrame || ! QuadEdgeSubdivision::isFrameVertex(v)) {
                edges->push_back(qe);
            }
        }
        QuadEdge* qd = &(qe->sym());
        const Vertex& vd = qd->orig();

        if(visitedVertices.find(vd) == visitedVertices.end()) {
            visitedVertices.insert(vd);
            if(includeFrame || ! QuadEdgeSubdivision::isFrameVertex(vd)) {
                edges->push_back(qd);
            }
        }
    }
    return edges;
}

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes
