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
 * Last port: geomgraph/DirectedEdgeStar.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/geomgraph/DirectedEdgeStar.h>
#include <geos/geomgraph/EdgeEndStar.h>
#include <geos/geomgraph/EdgeEnd.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geomgraph/EdgeRing.h>
#include <geos/geom/Position.h>
#include <geos/geom/Quadrant.h>
#include <geos/geom/Location.h>
#include <geos/util/TopologyException.h>
#include <geos/util.h>

#include <cassert>
#include <string>
#include <vector>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 1
#endif

//using namespace std;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

/*public*/
void
DirectedEdgeStar::insert(EdgeEnd* ee)
{
    assert(ee);
    DirectedEdge* de = detail::down_cast<DirectedEdge*>(ee);
    insertEdgeEnd(de);
}

/*public*/
int
DirectedEdgeStar::getOutgoingDegree()
{
    int degree = 0;
    for (EdgeEnd* ee: *this) {
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(ee);
        if(de->isInResult()) {
            ++degree;
        }
    }
    return degree;
}

/*public*/
int
DirectedEdgeStar::getOutgoingDegree(EdgeRing* er)
{
    int degree = 0;
    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        assert(*it);
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(*it);
        if(de->getEdgeRing() == er) {
            ++degree;
        }
    }
    return degree;
}

/*public*/
DirectedEdge*
DirectedEdgeStar::getRightmostEdge()
{
    EdgeEndStar::iterator it = begin();
    if(it == end()) {
        return nullptr;
    }

    assert(*it);
    DirectedEdge* de0 = detail::down_cast<DirectedEdge*>(*it);
    ++it;
    if(it == end()) {
        return de0;
    }

    it = end();
    --it;

    assert(*it);
    DirectedEdge* deLast = detail::down_cast<DirectedEdge*>(*it);

    assert(de0);
    int quad0 = de0->getQuadrant();
    assert(deLast);
    int quad1 = deLast->getQuadrant();
    if(Quadrant::isNorthern(quad0) && Quadrant::isNorthern(quad1)) {
        return de0;
    }
    else if(!Quadrant::isNorthern(quad0) && !Quadrant::isNorthern(quad1)) {
        return deLast;
    }
    else {
        // edges are in different hemispheres - make sure we return one that is non-horizontal
        //DirectedEdge *nonHorizontalEdge=NULL;
        if(de0->getDy() != 0) {
            return de0;
        }
        else if(deLast->getDy() != 0) {
            return deLast;
        }
    }
    assert(0); // found two horizontal edges incident on node
    return nullptr;
}

/*public*/
void
DirectedEdgeStar::computeLabelling(std::vector<GeometryGraph*>* geom)
//throw(TopologyException *)
{
    // this call can throw a TopologyException
    // we don't have any cleanup to do...
    EdgeEndStar::computeLabelling(geom);

    // determine the overall labelling for this DirectedEdgeStar
    // (i.e. for the node it is based at)
    label = Label(Location::NONE);
    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        EdgeEnd* ee = *it;
        assert(ee);
        Edge* e = ee->getEdge();
        assert(e);
        const Label& eLabel = e->getLabel();
        for(uint32_t i = 0; i < 2; ++i) {
            Location eLoc = eLabel.getLocation(i);
            if(eLoc == Location::INTERIOR || eLoc == Location::BOUNDARY) {
                label.setLocation(i, Location::INTERIOR);
            }
        }
    }
}

/*public*/
void
DirectedEdgeStar::mergeSymLabels()
{
    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        assert(*it);
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(*it);
        Label& deLabel = de->getLabel();

        DirectedEdge* deSym = de->getSym();
        assert(deSym);

        const Label& labelToMerge = deSym->getLabel();

        deLabel.merge(labelToMerge);
    }
}

/*public*/
void
DirectedEdgeStar::updateLabelling(const Label& nodeLabel)
{
    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        assert(*it);
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(*it);
        Label& deLabel = de->getLabel();
        deLabel.setAllLocationsIfNull(0, nodeLabel.getLocation(0));
        deLabel.setAllLocationsIfNull(1, nodeLabel.getLocation(1));
    }
}

/*private*/
const std::vector<DirectedEdge*>&
DirectedEdgeStar::getResultAreaEdges()
{
    if(resultAreaEdgesComputed) {
        return resultAreaEdgeList;
    }

    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        assert(*it);
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(*it);
        if(de->isInResult() || de->getSym()->isInResult()) {
            resultAreaEdgeList.push_back(de);
        }
    }

    resultAreaEdgesComputed = true;
    return resultAreaEdgeList;
}

/*public*/
void
DirectedEdgeStar::linkResultDirectedEdges()
// throw(TopologyException *)
{
    // make sure edges are copied to resultAreaEdges list
    getResultAreaEdges();
    // find first area edge (if any) to start linking at
    DirectedEdge* firstOut = nullptr;
    DirectedEdge* incoming = nullptr;
    int state = SCANNING_FOR_INCOMING;
    // link edges in CCW order
    for(DirectedEdge* nextOut : resultAreaEdgeList) {
        assert(nextOut);

        // skip de's that we're not interested in
        if(!nextOut->getLabel().isArea()) {
            continue;
        }

        DirectedEdge* nextIn = nextOut->getSym();
        assert(nextIn);

        // record first outgoing edge, in order to link the last incoming edge
        if(firstOut == nullptr && nextOut->isInResult()) {
            firstOut = nextOut;
        }

        switch(state) {
        case SCANNING_FOR_INCOMING:
            if(!nextIn->isInResult()) {
                continue;
            }
            incoming = nextIn;
            state = LINKING_TO_OUTGOING;
            break;
        case LINKING_TO_OUTGOING:
            if(!nextOut->isInResult()) {
                continue;
            }
            incoming->setNext(nextOut);
            state = SCANNING_FOR_INCOMING;
            break;
        }
    }
    if(state == LINKING_TO_OUTGOING) {
        if(firstOut == nullptr) {
            throw util::TopologyException("no outgoing dirEdge found",
                                          getCoordinate());
        }
        assert(firstOut->isInResult()); // unable to link last incoming dirEdge
        assert(incoming);
        incoming->setNext(firstOut);
    }
}

/*public*/
void
DirectedEdgeStar::linkMinimalDirectedEdges(EdgeRing* er)
{
    // find first area edge (if any) to start linking at
    DirectedEdge* firstOut = nullptr;
    DirectedEdge* incoming = nullptr;
    int state = SCANNING_FOR_INCOMING;

    // link edges in CW order
    for(std::vector<DirectedEdge*>::reverse_iterator
            i = resultAreaEdgeList.rbegin(), iEnd = resultAreaEdgeList.rend();
            i != iEnd;
            ++i) {
        DirectedEdge* nextOut = *i;
        assert(nextOut);

        DirectedEdge* nextIn = nextOut->getSym();
        assert(nextIn);

        // record first outgoing edge, in order to link the last incoming edge
        if(firstOut == nullptr && nextOut->getEdgeRing() == er) {
            firstOut = nextOut;
        }
        switch(state) {
        case SCANNING_FOR_INCOMING:
            if(nextIn->getEdgeRing() != er) {
                continue;
            }
            incoming = nextIn;
            state = LINKING_TO_OUTGOING;
            break;
        case LINKING_TO_OUTGOING:
            if(nextOut->getEdgeRing() != er) {
                continue;
            }
            assert(incoming);
            incoming->setNextMin(nextOut);
            state = SCANNING_FOR_INCOMING;
            break;
        }
    }
    if(state == LINKING_TO_OUTGOING) {
        assert(firstOut != nullptr); // found null for first outgoing dirEdge
        assert(firstOut->getEdgeRing() == er); // unable to link last incoming dirEdge
        assert(incoming);
        incoming->setNextMin(firstOut);
    }
}

/*public*/
void
DirectedEdgeStar::linkAllDirectedEdges()
{
    //getEdges();

    // find first area edge (if any) to start linking at
    DirectedEdge* prevOut = nullptr;
    DirectedEdge* firstIn = nullptr;

    // link edges in CW order
    EdgeEndStar::reverse_iterator rbeginIt = rbegin();
    EdgeEndStar::reverse_iterator rendIt = rend();
    for(EdgeEndStar::reverse_iterator it = rbeginIt; it != rendIt; ++it) {
        assert(*it);
        DirectedEdge* nextOut = detail::down_cast<DirectedEdge*>(*it);

        DirectedEdge* nextIn = nextOut->getSym();
        assert(nextIn);

        if(firstIn == nullptr) {
            firstIn = nextIn;
        }
        if(prevOut != nullptr) {
            nextIn->setNext(prevOut);
        }
        // record outgoing edge, in order to link the last incoming edge
        prevOut = nextOut;
    }
    assert(firstIn);
    firstIn->setNext(prevOut);
}

/*public*/
void
DirectedEdgeStar::findCoveredLineEdges()
{
    // Since edges are stored in CCW order around the node,
    // as we move around the ring we move from the right to the left side of the edge

    /*
     * Find first DirectedEdge of result area (if any).
     * The interior of the result is on the RHS of the edge,
     * so the start location will be:
     * - INTERIOR if the edge is outgoing
     * - EXTERIOR if the edge is incoming
     */
    Location startLoc = Location::NONE;

    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        assert(*it);
        DirectedEdge* nextOut = detail::down_cast<DirectedEdge*>(*it);

        DirectedEdge* nextIn = nextOut->getSym();
        assert(nextIn);

        if(!nextOut->isLineEdge()) {
            if(nextOut->isInResult()) {
                startLoc = Location::INTERIOR;
                break;
            }
            if(nextIn->isInResult()) {
                startLoc = Location::EXTERIOR;
                break;
            }
        }
    }

    // no A edges found, so can't determine if L edges are covered or not
    if(startLoc == Location::NONE) {
        return;
    }

    /*
     * move around ring, keeping track of the current location
     * (Interior or Exterior) for the result area.
     * If L edges are found, mark them as covered if they are in the interior
     */
    Location currLoc = startLoc;
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        assert(*it);
        DirectedEdge* nextOut = detail::down_cast<DirectedEdge*>(*it);

        DirectedEdge* nextIn = nextOut->getSym();
        assert(nextIn);

        if(nextOut->isLineEdge()) {
            nextOut->getEdge()->setCovered(currLoc == Location::INTERIOR);
        }
        else {    // edge is an Area edge
            if(nextOut->isInResult()) {
                currLoc = Location::EXTERIOR;
            }
            if(nextIn->isInResult()) {
                currLoc = Location::INTERIOR;
            }
        }
    }
}

/*public*/
void
DirectedEdgeStar::computeDepths(DirectedEdge* de)
{
    assert(de);

    EdgeEndStar::iterator edgeIterator = find(de);
    //The LT operator used for building std::set
    //uses geos::algorithm::RobustDeterminant::signOfDet2x2  to compute and compare
    //direction of two edges. Which becomes unstable and looses commutativity
    //on almost collinear edges. So set becomes broken in terms of less
    //and find(de) fails, when de is taken from set itself
    //but finding element is still possible using normal iteration.
    if (edgeIterator == end()) {
        for (edgeIterator = begin(); edgeIterator != end(); ++edgeIterator) {
            if ((*edgeIterator)->compareTo(de) == 0) {
                break;
            }
        }
        if (edgeIterator == end()) {
            throw util::TopologyException("Precision error comparing edges at ", de->getCoordinate());
        }
    }

    int startDepth = de->getDepth(Position::LEFT);
    int targetLastDepth = de->getDepth(Position::RIGHT);

    // compute the depths from this edge up to the end of the edge array
    EdgeEndStar::iterator nextEdgeIterator = edgeIterator;
    ++nextEdgeIterator;
    int nextDepth = computeDepths(nextEdgeIterator, end(), startDepth);

    // compute the depths for the initial part of the array
    int lastDepth = computeDepths(begin(), edgeIterator, nextDepth);

    if(lastDepth != targetLastDepth) {
        throw util::TopologyException("depth mismatch at ", de->getCoordinate());
    }
}

/*public*/
int
DirectedEdgeStar::computeDepths(EdgeEndStar::iterator startIt,
                                EdgeEndStar::iterator endIt, int startDepth)
{
    int currDepth = startDepth;
    for(EdgeEndStar::iterator it = startIt; it != endIt; ++it) {
        assert(*it);
        DirectedEdge* nextDe = detail::down_cast<DirectedEdge*>(*it);

        nextDe->setEdgeDepths(Position::RIGHT, currDepth);
        currDepth = nextDe->getDepth(Position::LEFT);
    }
    return currDepth;
}

/*public*/
std::string
DirectedEdgeStar::print() const
{
    std::string out = "DirectedEdgeStar: " + getCoordinate().toString();

    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        assert(*it);
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(*it);
        assert(de);
        out += "out ";
        out += de->print();
        out += "\n";
        out += "in ";
        assert(de->getSym());
        out += de->getSym()->print();
        out += "\n";
    }
    return out;
}

} // namespace geos.geomgraph
} // namespace geos
