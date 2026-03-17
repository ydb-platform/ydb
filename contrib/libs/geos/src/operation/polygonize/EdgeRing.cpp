/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: operation/polygonize/EdgeRing.java 0b3c7e3eb0d3e
 *
 **********************************************************************/

#include <geos/operation/polygonize/EdgeRing.h>
#include <geos/operation/polygonize/PolygonizeEdge.h>
#include <geos/planargraph/DirectedEdge.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/Orientation.h>
#include <geos/util/IllegalArgumentException.h>
#include <geos/util.h> // TODO: drop this, includes too much
#include <geos/index/strtree/STRtree.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/geom/Location.h>

#include <vector>
#include <cassert>

//#define DEBUG_ALLOC 1
//#define GEOS_PARANOIA_LEVEL 2

using namespace std;
using namespace geos::planargraph;
using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace polygonize { // geos.operation.polygonize

/*public*/
EdgeRing*
EdgeRing::findEdgeRingContaining(const std::vector<EdgeRing*> & erList)
{
    const LinearRing* testRing = getRingInternal();
    if(! testRing) {
        return nullptr;
    }
    const Envelope* testEnv = testRing->getEnvelopeInternal();
    EdgeRing* minRing = nullptr;
    const Envelope* minRingEnv = nullptr;

    for(auto& tryEdgeRing : erList) {
        auto tryRing = tryEdgeRing->getRingInternal();
        auto tryShellEnv = tryRing->getEnvelopeInternal();
        // the hole envelope cannot equal the shell envelope
        // (also guards against testing rings against themselves)
        if (tryShellEnv->equals(testEnv)) {
            continue;
        }
        // hole must be contained in shell
        if (!tryShellEnv->contains(testEnv)) {
            continue;
        }

        auto tryCoords = tryRing->getCoordinatesRO();
        const Coordinate& testPt = ptNotInList(testRing->getCoordinatesRO(), tryCoords);

        // check if this new containing ring is smaller than the current minimum ring
        if(tryEdgeRing->isInRing(testPt)) {
            if(minRing == nullptr || minRingEnv->contains(tryShellEnv)) {
                minRing = tryEdgeRing;
                minRingEnv = minRing->getRingInternal()->getEnvelopeInternal();
            }
        }
    }
    return minRing;
}

std::vector<PolygonizeDirectedEdge*>
EdgeRing::findDirEdgesInRing(PolygonizeDirectedEdge* startDE) {
    auto de = startDE;
    std::vector<decltype(de)> edges;

    do {
        edges.push_back(de);
        de = de->getNext();
    } while (de != startDE);

    return edges;
}

/*public static*/
const Coordinate&
EdgeRing::ptNotInList(const CoordinateSequence* testPts,
                      const CoordinateSequence* pts)
{
    const std::size_t npts = testPts->getSize();
    for(std::size_t i = 0; i < npts; ++i) {
        const Coordinate& testPt = testPts->getAt(i);
        if(!isInList(testPt, pts)) {
            return testPt;
        }
    }
    return Coordinate::getNull();
}

/*public static*/
bool
EdgeRing::isInList(const Coordinate& pt,
                   const CoordinateSequence* pts)
{
    const std::size_t npts = pts->getSize();
    for(std::size_t i = 0; i < npts; ++i) {
        if(pt == pts->getAt(i)) {
            return true;
        }
    }
    return false;
}

/*public*/
EdgeRing::EdgeRing(const GeometryFactory* newFactory)
    :
    factory(newFactory),
    ring(nullptr),
    ringPts(nullptr),
    holes(nullptr),
    is_hole(false)
{
#ifdef DEBUG_ALLOC
    cerr << "[" << this << "] EdgeRing(factory)" << endl;
#endif // DEBUG_ALLOC
}

void
EdgeRing::build(PolygonizeDirectedEdge* startDE) {
    auto de = startDE;
    do {
        add(de);
        de->setRing(this);
        de = de->getNext();
    } while (de != startDE);
}

/*public*/
void
EdgeRing::add(const PolygonizeDirectedEdge* de)
{
    deList.push_back(de);
}

/*public*/
void
EdgeRing::computeHole()
{
    getRingInternal();
    is_hole = Orientation::isCCW(ring->getCoordinatesRO());
}

/*public*/
void
EdgeRing::addHole(LinearRing* hole)
{
    if(holes == nullptr) {
        holes.reset(new std::vector<std::unique_ptr<LinearRing>>());
    }
    holes->emplace_back(hole);
}

void
EdgeRing::addHole(EdgeRing* holeER) {
    holeER->setShell(this);
    auto hole = holeER->getRingOwnership(); // TODO is this right method?
    addHole(hole.release());
}

/*public*/
std::unique_ptr<Polygon>
EdgeRing::getPolygon()
{
    if (holes) {
        return factory->createPolygon(std::move(ring), std::move(*holes));
    } else {
        return factory->createPolygon(std::move(ring));
    }
}

/*public*/
bool
EdgeRing::isValid()
{
    if(! getRingInternal()) {
        return false;    // computes cached ring
    }
    return ring->isValid();
}

/*private*/
const CoordinateSequence*
EdgeRing::getCoordinates()
{
    if(ringPts == nullptr) {
        ringPts = detail::make_unique<CoordinateArraySequence>(0, 0);
        for(const auto& de : deList) {
            auto edge = dynamic_cast<PolygonizeEdge*>(de->getEdge());
            addEdge(edge->getLine()->getCoordinatesRO(),
                    de->getEdgeDirection(), ringPts.get());
        }
    }
    return ringPts.get();
}

/*public*/
std::unique_ptr<LineString>
EdgeRing::getLineString()
{
    getCoordinates();
    return std::unique_ptr<LineString>(factory->createLineString(*ringPts));
}

/*public*/
LinearRing*
EdgeRing::getRingInternal()
{
    if(ring != nullptr) {
        return ring.get();
    }

    getCoordinates();
    try {
        ring.reset(factory->createLinearRing(*ringPts));
    }
    catch(const geos::util::IllegalArgumentException& e) {
#if GEOS_DEBUG
        // FIXME: print also ringPts
        std::cerr << "EdgeRing::getRingInternal: "
                  << e.what()
                  << endl;
#endif
        ::geos::ignore_unused_variable_warning(e);
    }
    return ring.get();
}

/*public*/
std::unique_ptr<LinearRing>
EdgeRing::getRingOwnership()
{
    getRingInternal(); // active lazy generation
    return std::move(ring);
}

/*private*/
void
EdgeRing::addEdge(const CoordinateSequence* coords, bool isForward,
                  CoordinateArraySequence* coordList)
{
    const std::size_t npts = coords->getSize();
    if(isForward) {
        for(std::size_t i = 0; i < npts; ++i) {
            coordList->add(coords->getAt(i), false);
        }
    }
    else {
        for(std::size_t i = npts; i > 0; --i) {
            coordList->add(coords->getAt(i - 1), false);
        }
    }
}

EdgeRing*
EdgeRing::getOuterHole() const {
    // Only shells can have outer holes
    if (isHole()) {
        return nullptr;
    }

    // A shell is an outer shell if any edge is also in an outer hole.
    // A hole is an outer shell if it is not contained by a shell.
    for (auto& de : deList) {
        auto adjRing = (dynamic_cast<PolygonizeDirectedEdge*>(de->getSym()))->getRing();
        if (adjRing->isOuterHole()) {
            return adjRing;
        }
    }

    return nullptr;
}

void
EdgeRing::updateIncludedRecursive() {
    visitedByUpdateIncludedRecursive = true;

    if (isHole()) {
        return;
    }

    for (const auto& de : deList) {
        auto adjShell = (dynamic_cast<const PolygonizeDirectedEdge*>(de->getSym()))->getRing()->getShell();

        if (adjShell != nullptr) {
            if (!adjShell->isIncludedSet() && !adjShell->visitedByUpdateIncludedRecursive) {
                adjShell->updateIncludedRecursive();
            }
        }
    }

    for (const auto& de : deList) {
        auto adjShell = (dynamic_cast<const PolygonizeDirectedEdge*>(de->getSym()))->getRing()->getShell();

        if (adjShell != nullptr) {
            if (adjShell->isIncludedSet()) {
                setIncluded(!adjShell->isIncluded());
                return;
            }
        }
    }

}

} // namespace geos.operation.polygonize
} // namespace geos.operation
} // namespace geos
