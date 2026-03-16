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
 * Last port: geomgraph/EdgeRing.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/util/Assert.h>
#include <geos/util/TopologyException.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/Orientation.h>
#include <geos/geomgraph/EdgeRing.h>
#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geomgraph/DirectedEdgeStar.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/Label.h>
#include <geos/geom/Position.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Location.h>
#include <geos/geom/Envelope.h>
#include <geos/util.h>

#include <vector>
#include <cassert>
#include <iostream> // for operator<<

using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

EdgeRing::EdgeRing(DirectedEdge* newStart,
                   const GeometryFactory* newGeometryFactory)
    :
    startDe(newStart),
    geometryFactory(newGeometryFactory),
    holes(),
    maxNodeDegree(-1),
    edges(),
    label(Location::NONE), // new Label(Location::NONE)),
    ring(nullptr),
    isHoleVar(false),
    shell(nullptr)
{
    /*
     * Commented out to fix different polymorphism in C++ (from Java)
     * Make sure these calls are made by derived classes !
     */
    //computePoints(start);
    //computeRing();
#ifdef GEOS_DEBUG
    cerr << "EdgeRing[" << this << "] ctor" << endl;
#endif
    testInvariant();
}

bool
EdgeRing::isIsolated()
{
    testInvariant();
    return (label.getGeometryCount() == 1);
}

bool
EdgeRing::isHole()
{
    testInvariant();

    // We can't tell if this is an hole
    // unless we computed the ring
    // see computeRing()
    assert(ring);

    return isHoleVar;
}


LinearRing*
EdgeRing::getLinearRing()
{
    testInvariant();
    return ring.get();
}

Label&
EdgeRing::getLabel()
{
    testInvariant();
    return label;
}

bool
EdgeRing::isShell()
{
    testInvariant();
    return (shell == nullptr);
}

EdgeRing*
EdgeRing::getShell()
{
    testInvariant();
    return shell;
}

void
EdgeRing::setShell(EdgeRing* newShell)
{
    shell = newShell;
    if(shell != nullptr) {
        shell->addHole(this);
    }
    testInvariant();
}

void
EdgeRing::addHole(EdgeRing* edgeRing)
{
    holes.emplace_back(edgeRing);
    testInvariant();
}

/*public*/
std::unique_ptr<Polygon>
EdgeRing::toPolygon(const GeometryFactory* p_geometryFactory)
{
    testInvariant();

    // We don't use "clone" here because
    // GeometryFactory::createPolygon really
    // wants a LinearRing
    auto shellLR = detail::make_unique<LinearRing>(*(getLinearRing()));
    if (holes.empty()) {
        return p_geometryFactory->createPolygon(std::move(shellLR));
    } else {
        size_t nholes = holes.size();
        std::vector<std::unique_ptr<LinearRing>> holeLR(nholes);
        for(size_t i = 0; i < nholes; ++i) {
            holeLR[i] = detail::make_unique<LinearRing>(*(holes[i]->getLinearRing()));
        }

        return p_geometryFactory->createPolygon(std::move(shellLR), std::move(holeLR));
    }
}

/*public*/
void
EdgeRing::computeRing()
{
    testInvariant();

    if(ring != nullptr) {
        return;    // don't compute more than once
    }
    auto coordSeq = geometryFactory->getCoordinateSequenceFactory()->create(std::move(pts));
    ring = geometryFactory->createLinearRing(std::move(coordSeq));
    isHoleVar = Orientation::isCCW(ring->getCoordinatesRO());

    testInvariant();
}

/*public*/
std::vector<DirectedEdge*>&
EdgeRing::getEdges()
{
    testInvariant();

    return edges;
}

/*protected*/
void
EdgeRing::computePoints(DirectedEdge* newStart)
// throw(const TopologyException &)
{
    startDe = newStart;
    DirectedEdge* de = newStart;
    bool isFirstEdge = true;
    do {
        //util::Assert::isTrue(de!=NULL,"EdgeRing::computePoints: found null Directed Edge");
        //assert(de!=NULL); // EdgeRing::computePoints: found null Directed Edge
        if(de == nullptr)
            throw util::TopologyException(
                "EdgeRing::computePoints: found null Directed Edge");

        if(de->getEdgeRing() == this)
            throw util::TopologyException(
                "Directed Edge visited twice during ring-building",
                de->getCoordinate());

        edges.push_back(de);
        const Label& deLabel = de->getLabel();
        assert(deLabel.isArea());
        mergeLabel(deLabel);
        addPoints(de->getEdge(), de->isForward(), isFirstEdge);
        isFirstEdge = false;
        setEdgeRing(de, this);
        de = getNext(de);
    }
    while(de != startDe);

    testInvariant();

}

/*public*/
int
EdgeRing::getMaxNodeDegree()
{

    testInvariant();

    if(maxNodeDegree < 0) {
        computeMaxNodeDegree();
    }
    return maxNodeDegree;
}

/*private*/
void
EdgeRing::computeMaxNodeDegree()
{
    maxNodeDegree = 0;
    DirectedEdge* de = startDe;
    do {
        Node* node = de->getNode();
        EdgeEndStar* ees = node->getEdges();
        DirectedEdgeStar* des = detail::down_cast<DirectedEdgeStar*>(ees);
        int degree = des->getOutgoingDegree(this);
        if(degree > maxNodeDegree) {
            maxNodeDegree = degree;
        }
        de = getNext(de);
    }
    while(de != startDe);
    maxNodeDegree *= 2;

    testInvariant();

}

/*public*/
void
EdgeRing::setInResult()
{
    DirectedEdge* de = startDe;
    do {
        de->getEdge()->setInResult(true);
        de = de->getNext();
    }
    while(de != startDe);

    testInvariant();

}

/*protected*/
void
EdgeRing::mergeLabel(const Label& deLabel)
{
    mergeLabel(deLabel, 0);
    mergeLabel(deLabel, 1);

    testInvariant();

}

/*protected*/
void
EdgeRing::mergeLabel(const Label& deLabel, int geomIndex)
{

    testInvariant();

    Location loc = deLabel.getLocation(geomIndex, Position::RIGHT);
    // no information to be had from this label
    if(loc == Location::NONE) {
        return;
    }

    // if there is no current RHS value, set it
    if(label.getLocation(geomIndex) == Location::NONE) {
        label.setLocation(geomIndex, loc);
        return;
    }
}

/*protected*/
void
EdgeRing::addPoints(Edge* edge, bool isForward, bool isFirstEdge)
{
    // EdgeRing::addPoints: can't add points after LinearRing construction
    assert(ring == nullptr);

    assert(edge);
    const CoordinateSequence* edgePts = edge->getCoordinates();

    assert(edgePts);
    size_t numEdgePts = edgePts->getSize();

    if(isForward) {
        if(isFirstEdge) {
            edgePts->toVector(pts);
            return;
        } else {
            for(size_t i = 1; i < numEdgePts; ++i) {
                pts.push_back(edgePts->getAt(i));
            }
        }
    }

    else { // is backward
        size_t startIndex = numEdgePts - 1;
        if(isFirstEdge) {
            startIndex = numEdgePts;
        }
        for(size_t i = startIndex; i > 0; --i) {
            pts.push_back(edgePts->getAt(i - 1));
        }
    }

    testInvariant();
}

/*public*/
bool
EdgeRing::containsPoint(const Coordinate& p)
{
    testInvariant();

    assert(ring);

    const Envelope* env = ring->getEnvelopeInternal();
    assert(env);
    if(! env->contains(p)) {
        return false;
    }

    if(! PointLocation::isInRing(p, ring->getCoordinatesRO())) {
        return false;
    }

    for(const auto& hole : holes) {
        assert(hole);
        if(hole->containsPoint(p)) {
            return false;
        }
    }
    return true;
}

std::ostream&
operator<< (std::ostream& os, const EdgeRing& er)
{
    os << "EdgeRing[" << &er << "]: "
       << std::endl;
    return os;
}

} // namespace geos.geomgraph
} // namespace geos

