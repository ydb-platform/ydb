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
 * Last port: geomgraph/DirectedEdge.java r428 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/util/TopologyException.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/EdgeRing.h> // for printing
#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geom/Location.h>
#include <geos/geomgraph/Label.h>
#include <geos/geom/Position.h>

#include <geos/inline.h>

#include <cmath>
#include <cassert>
#include <string>
#include <sstream>

using namespace geos::geom;

#ifndef GEOS_INLINE
# include "geos/geomgraph/DirectedEdge.inl"
#endif

namespace geos {
namespace geomgraph { // geos.geomgraph

/* public static */
int
DirectedEdge::depthFactor(Location currLocation, Location nextLocation)
{
    if(currLocation == Location::EXTERIOR && nextLocation == Location::INTERIOR) {
        return 1;
    }
    else if(currLocation == Location::INTERIOR && nextLocation == Location::EXTERIOR) {
        return -1;
    }
    return 0;
}

#if 0
DirectedEdge::DirectedEdge():
    EdgeEnd(),
    isInResultVar(false),
    isVisitedVar(false),
    sym(NULL),
    next(NULL),
    nextMin(NULL),
    edgeRing(NULL),
    minEdgeRing(NULL)
{
    depth[0] = 0;
    depth[1] = -999;
    depth[2] = -999;

}
#endif

DirectedEdge::DirectedEdge(Edge* newEdge, bool newIsForward):
    EdgeEnd(newEdge),
    isForwardVar(newIsForward),
    isInResultVar(false),
    isVisitedVar(false),
    sym(nullptr),
    next(nullptr),
    nextMin(nullptr),
    edgeRing(nullptr),
    minEdgeRing(nullptr)
{
    depth[0] = 0;
    depth[1] = -999;
    depth[2] = -999;

    assert(newEdge);
    assert(newEdge->getNumPoints() >= 2);

    if(isForwardVar) {
        init(edge->getCoordinate(0), edge->getCoordinate(1));
    }
    else {
        auto  n = edge->getNumPoints() - 1;
        init(edge->getCoordinate(n), edge->getCoordinate(n - 1));
    }
    computeDirectedLabel();
}


/*public*/
void
DirectedEdge::setDepth(int position, int newDepth)
{
    if(depth[position] != -999) {
        if(depth[position] != newDepth) {
            throw util::TopologyException("assigned depths do not match", getCoordinate());
        }
        //Assert.isTrue(depth[position] == depthVal, "assigned depths do not match at " + getCoordinate());
    }
    depth[position] = newDepth;
}

/*public*/
int
DirectedEdge::getDepthDelta() const
{
    int depthDelta = edge->getDepthDelta();
    if(!isForwardVar) {
        depthDelta = -depthDelta;
    }
    return depthDelta;
}

/*public*/
void
DirectedEdge::setVisitedEdge(bool newIsVisited)
{
    setVisited(newIsVisited);
    assert(sym);
    sym->setVisited(newIsVisited);
}


/*public*/
bool
DirectedEdge::isLineEdge()
{
    bool isLine = label.isLine(0) || label.isLine(1);
    bool isExteriorIfArea0 = !label.isArea(0) || label.allPositionsEqual(0, Location::EXTERIOR);
    bool isExteriorIfArea1 = !label.isArea(1) || label.allPositionsEqual(1, Location::EXTERIOR);
    return isLine && isExteriorIfArea0 && isExteriorIfArea1;
}

/*public*/
bool
DirectedEdge::isInteriorAreaEdge()
{
    bool p_isInteriorAreaEdge = true;
    for(uint32_t i = 0; i < 2; i++) {
        if(!(label.isArea(i)
                && label.getLocation(i, Position::LEFT) == Location::INTERIOR
                && label.getLocation(i, Position::RIGHT) == Location::INTERIOR)) {
            p_isInteriorAreaEdge = false;
        }
    }
    return p_isInteriorAreaEdge;
}

/*private*/
void
DirectedEdge::computeDirectedLabel()
{
    label = edge->getLabel();
    if(!isForwardVar) {
        label.flip();
    }
}

/*public*/
void
DirectedEdge::setEdgeDepths(int position, int newDepth)
{
    // get the depth transition delta from R to L for this directed Edge
    int depthDelta = getEdge()->getDepthDelta();
    if(!isForwardVar) {
        depthDelta = -depthDelta;
    }
    // if moving from L to R instead of R to L must change sign of delta
    int directionFactor = 1;
    if(position == Position::LEFT) {
        directionFactor = -1;
    }
    int oppositePos = Position::opposite(position);
    int delta = depthDelta * directionFactor;
    //TESTINGint delta = depthDelta * DirectedEdge.depthFactor(loc, oppositeLoc);
    int oppositeDepth = newDepth + delta;
    setDepth(position, newDepth);
    setDepth(oppositePos, oppositeDepth);
}

/*public*/
std::string
DirectedEdge::print() const
{
    std::stringstream ss;
    ss << EdgeEnd::print();
    ss << " "
       << depth[Position::LEFT]
       << "/"
       << depth[Position::RIGHT]
       << " ("
       << getDepthDelta()
       << ")";
    if(isInResultVar) {
        ss << " inResult";
    }
    ss << " EdgeRing: " << edgeRing;
    if(edgeRing) {
        EdgeRing* er = edgeRing;
        ss << " (" << *er << ")";
    }
    return ss.str();
}

/*public*/
std::string
DirectedEdge::printEdge()
{
    //std::string out=print();
    std::string out("");
    if(isForwardVar) {
        out += edge->print();
    }
    else {
        out += edge->printReverse();
    }
    return out;
}

} // namespace geos.geomgraph
} // namespace geos

