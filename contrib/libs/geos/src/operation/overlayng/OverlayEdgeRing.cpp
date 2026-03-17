/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/overlayng/OverlayEdgeRing.h>
#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/geom/Location.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/util/TopologyException.h>
#include <geos/algorithm/locate/PointOnGeometryLocator.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/algorithm/Orientation.h>
#include <geos/operation/polygonize/EdgeRing.h>

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;
using geos::operation::polygonize::EdgeRing;



OverlayEdgeRing::OverlayEdgeRing(OverlayEdge* start, const GeometryFactory* geometryFactory)
    : startEdge(start)
    , ring(nullptr)
    , m_isHole(false)
    , locator(nullptr)
    , shell(nullptr)
{
    computeRingPts(start, ringPts);
    computeRing(ringPts, geometryFactory);
}

/*public*/
std::unique_ptr<LinearRing>
OverlayEdgeRing::getRing()
{
    return std::move(ring);
}

const LinearRing*
OverlayEdgeRing::getRingPtr() const
{
    return ring.get();
}

/**
* Tests whether this ring is a hole.
* @return <code>true</code> if this ring is a hole
*/
/*public*/
bool
OverlayEdgeRing::isHole() const
{
    return m_isHole;
}

/**
* Sets the containing shell ring of a ring that has been determined to be a hole.
*
* @param shell the shell ring
*/
/*public*/
void
OverlayEdgeRing::setShell(OverlayEdgeRing* p_shell)
{
    shell = p_shell;
    if (shell != nullptr)
        shell->addHole(this);
}

/**
* Tests whether this ring has a shell assigned to it.
*
* @return true if the ring has a shell
*/
/*public*/
bool
OverlayEdgeRing::hasShell() const
{
    return shell != nullptr;
}

/**
* Gets the shell for this ring.  The shell is the ring itself if it is not a hole, otherwise its parent shell.
*
* @return the shell for this ring
*/
/*public*/
const OverlayEdgeRing*
OverlayEdgeRing::getShell() const
{
    if (isHole())
        return shell;
    return this;
}

/*public*/
void
OverlayEdgeRing::addHole(OverlayEdgeRing* p_ring)
{
    holes.push_back(p_ring);
}

void
OverlayEdgeRing::closeRing(CoordinateArraySequence& pts)
{
    if(pts.size() > 0) {
        pts.add(pts.getAt(0), false);
    }
}

/*private*/
void
OverlayEdgeRing::computeRingPts(OverlayEdge* start, CoordinateArraySequence& pts)
{
    OverlayEdge* edge = start;
    do {
        if (edge->getEdgeRing() == this)
            throw util::TopologyException("Edge visited twice during ring-building", edge->getCoordinate());
            // only valid for polygonal output

        edge->addCoordinates(&pts);
        edge->setEdgeRing(this);
        if (edge->nextResult() == nullptr)
            throw util::TopologyException("Found null edge in ring", edge->dest());

        edge = edge->nextResult();
    }
    while (edge != start);
    closeRing(pts);
    return;
}

/*private*/
void
OverlayEdgeRing::computeRing(const CoordinateArraySequence& p_ringPts, const GeometryFactory* geometryFactory)
{
    if (ring != nullptr) return;   // don't compute more than once
    ring.reset(geometryFactory->createLinearRing(p_ringPts));
    m_isHole = algorithm::Orientation::isCCW(ring->getCoordinatesRO());
}

/**
* Computes the list of coordinates which are contained in this ring.
* The coordinates are computed once only and cached.
*
* @return an array of the {@link Coordinate}s in this ring
*/
/*private*/
const CoordinateArraySequence&
OverlayEdgeRing::getCoordinates()
{
    return ringPts;
}

/**
* Finds the innermost enclosing shell OverlayEdgeRing
* containing this OverlayEdgeRing, if any.
* The innermost enclosing ring is the smallest enclosing ring.
* The algorithm used depends on the fact that:
*  ring A contains ring B iff envelope(ring A) contains envelope(ring B)
*
* This routine is only safe to use if the chosen point of the hole
* is known to be properly contained in a shell
* (which is guaranteed to be the case if the hole does not touch its shell)
*
* To improve performance of this function the caller should
* make the passed shellList as small as possible (e.g.
* by using a spatial index filter beforehand).
*
* @return containing EdgeRing, if there is one
* or null if no containing EdgeRing is found
*/
/*public*/
OverlayEdgeRing*
OverlayEdgeRing::findEdgeRingContaining(std::vector<OverlayEdgeRing*>& erList)
{
    const LinearRing* testRing = ring.get();
    const Envelope* testEnv = testRing->getEnvelopeInternal();

    OverlayEdgeRing* minRing = nullptr;
    const Envelope* minRingEnv = nullptr;
    for (auto tryEdgeRing: erList) {
        const LinearRing* tryRing = tryEdgeRing->getRingPtr();
        const Envelope* tryShellEnv = tryRing->getEnvelopeInternal();
        // the hole envelope cannot equal the shell envelope
        // (also guards against testing rings against themselves)
        if (tryShellEnv->equals(testEnv)) continue;

        // hole must be contained in shell
        if (! tryShellEnv->contains(testEnv)) continue;

        const Coordinate& testPt = EdgeRing::ptNotInList(testRing->getCoordinatesRO(), tryRing->getCoordinatesRO());
        bool isContained = tryEdgeRing->isInRing(testPt);
        // check if the new containing ring is smaller than the current minimum ring
        if (isContained) {
            if (minRing == nullptr || minRingEnv->contains(tryShellEnv)) {
                minRing = tryEdgeRing;
                minRingEnv = minRing->getRingPtr()->getEnvelopeInternal();
            }
        }
    }
    return minRing;
}

/*private*/
PointOnGeometryLocator*
OverlayEdgeRing::getLocator()
{
    if (locator == nullptr) {
      locator.reset(new IndexedPointInAreaLocator(*(getRingPtr())));
    }
    return locator.get();
}

/*public*/
bool
OverlayEdgeRing::isInRing(const Coordinate& pt)
{
    /**
    * Use an indexed point-in-polygon for performance
    */
    return Location::EXTERIOR != getLocator()->locate(&pt);
}

/*public*/
const Coordinate&
OverlayEdgeRing::getCoordinate()
{
    return ringPts.getAt(0);
}

/**
* Computes the {@link Polygon} formed by this ring and any contained holes.
* @return the {@link Polygon} formed by this ring and its holes.
*/
/*public*/
std::unique_ptr<Polygon>
OverlayEdgeRing::toPolygon(const GeometryFactory* factory)
{
    std::vector<std::unique_ptr<LinearRing>> holeLR;
    if (holes.size() > 0) {
        for (std::size_t i = 0; i < holes.size(); i++) {
            std::unique_ptr<LinearRing> r = holes[i]->getRing();
            holeLR.push_back(std::move(r));
        }
    }
    return factory->createPolygon(std::move(ring), std::move(holeLR));
}

/*public*/
OverlayEdge*
OverlayEdgeRing::getEdge()
{
    return startEdge;
}




} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
