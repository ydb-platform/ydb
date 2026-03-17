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
 * Last port: geomgraph/Edge.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

#include <cassert>
#include <string>
#include <sstream>

#include <geos/geomgraph/Edge.h>
#include <geos/geom/Position.h>
#include <geos/geomgraph/Label.h>
#include <geos/geomgraph/index/MonotoneChainEdge.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequence.h> // FIXME: shouldn't use
#include <geos/geom/Coordinate.h>
#include <geos/util.h>

//#define GEOS_DEBUG_INTERSECT 0
#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG || GEOS_DEBUG_INTERSECT
#include <iostream>
#endif

using namespace std;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

using namespace geos::geomgraph::index;
using namespace geos::algorithm;

/**
 * Updates an IM from the label for an edge.
 * Handles edges from both L and A geometrys.
 */
void
Edge::updateIM(const Label& lbl, IntersectionMatrix& im)
{
    im.setAtLeastIfValid(lbl.getLocation(0, Position::ON),
                         lbl.getLocation(1, Position::ON),
                         1);
    if(lbl.isArea()) {

        im.setAtLeastIfValid(lbl.getLocation(0, Position::LEFT),
                             lbl.getLocation(1, Position::LEFT),
                             2);

        im.setAtLeastIfValid(lbl.getLocation(0, Position::RIGHT),
                             lbl.getLocation(1, Position::RIGHT),
                             2);
    }
}

/*public*/
Edge::~Edge() = default;

/*public*/
Edge::Edge(CoordinateSequence* newPts, const Label& newLabel)
    :
    GraphComponent(newLabel),
    mce(nullptr),
    env(newPts->getEnvelope()),
    depth(),
    depthDelta(0),
    isIsolatedVar(true),
    pts(newPts),
    eiList(this)
{
    testInvariant();
}

/*public*/
Edge::Edge(CoordinateSequence* newPts)
    :
    GraphComponent(),
    mce(nullptr),
    env(newPts->getEnvelope()),
    depth(),
    depthDelta(0),
    isIsolatedVar(true),
    pts(newPts),
    eiList(this)
{
    testInvariant();
}

/*public*/
MonotoneChainEdge*
Edge::getMonotoneChainEdge()
{
    testInvariant();
    if(mce == nullptr) {
        mce = detail::make_unique<MonotoneChainEdge>(this);
    }
    return mce.get();
}


/*public*/
bool
Edge::isCollapsed() const
{
    testInvariant();
    if(!label.isArea()) {
        return false;
    }
    if(getNumPoints() != 3) {
        return false;
    }
    if(pts->getAt(0) == pts->getAt(2)) {
        return true;
    }
    return false;
}

Edge*
Edge::getCollapsedEdge()
{
    testInvariant();
    CoordinateSequence* newPts = new CoordinateArraySequence(2);
    newPts->setAt(pts->getAt(0), 0);
    newPts->setAt(pts->getAt(1), 1);
    return new Edge(newPts, Label::toLineLabel(label));
}

/*public*/
void
Edge::addIntersections(LineIntersector* li, size_t segmentIndex, size_t geomIndex)
{
#if GEOS_DEBUG
    cerr << "[" << this << "] Edge::addIntersections(" << li->toString() << ", " << segmentIndex << ", " << geomIndex <<
         ") called" << endl;
#endif
    for(size_t i = 0; i < li->getIntersectionNum(); ++i) {
        addIntersection(li, segmentIndex, geomIndex, i);
    }

    testInvariant();
}

/*public*/
void
Edge::addIntersection(LineIntersector* li,
                      size_t segmentIndex, size_t geomIndex, size_t intIndex)
{
#if GEOS_DEBUG
    std::cerr << "[" << this << "] Edge::addIntersection(" << li->toString() << ", " << segmentIndex << ", " << geomIndex <<
              ", " << intIndex << ") called" << std::endl;
#endif
    const Coordinate& intPt = li->getIntersection(intIndex);
    auto normalizedSegmentIndex = segmentIndex;
    double dist = li->getEdgeDistance(geomIndex, intIndex);

    // normalize the intersection point location
    auto nextSegIndex = normalizedSegmentIndex + 1;
    auto npts = getNumPoints();
    if(nextSegIndex < npts) {
        const Coordinate& nextPt = pts->getAt(nextSegIndex);
        // Normalize segment index if intPt falls on vertex
        // The check for point equality is 2D only - Z values are ignored
        if(intPt.equals2D(nextPt)) {
            normalizedSegmentIndex = nextSegIndex;
            dist = 0.0;
        }
    }

    /*
     * Add the intersection point to edge intersection list.
     */
#if GEOS_DEBUG
    cerr << "Edge::addIntersection adding to edge intersection list point " << intPt.toString() << endl;
#endif
    eiList.add(intPt, normalizedSegmentIndex, dist);

    testInvariant();
}

/*public*/
bool
Edge::equals(const Edge& e) const
{
    testInvariant();

    auto npts1 = getNumPoints();
    auto npts2 = e.getNumPoints();

    if(npts1 != npts2) {
        return false;
    }

    bool isEqualForward = true;
    bool isEqualReverse = true;

    for(size_t i = 0, iRev = npts1 - 1; i < npts1; ++i, --iRev) {
        const Coordinate& e1pi = pts->getAt(i);
        const Coordinate& e2pi = e.pts->getAt(i);
        const Coordinate& e2piRev = e.pts->getAt(iRev);

        if(!e1pi.equals2D(e2pi)) {
            isEqualForward = false;
        }
        if(!e1pi.equals2D(e2piRev)) {
            isEqualReverse = false;
        }
        if(!isEqualForward && !isEqualReverse) {
            return false;
        }
    }
    return true;
}

/*public*/
bool
Edge::isPointwiseEqual(const Edge* e) const
{
    testInvariant();

#if GEOS_DEBUG > 2
    cerr << "Edge::isPointwiseEqual call" << endl;
#endif
    auto npts = getNumPoints();
    auto enpts = e->getNumPoints();
    if(npts != enpts) {
        return false;
    }
#if GEOS_DEBUG
    cerr << "Edge::isPointwiseEqual scanning " << enpts << "x" << npts << " points" << endl;
#endif
    for(unsigned int i = 0; i < npts; ++i) {
        if(!pts->getAt(i).equals2D(e->pts->getAt(i))) {
            return false;
        }
    }
    return true;
}

string
Edge::print() const
{
    testInvariant();

    std::stringstream ss;
    ss << *this;
    return ss.str();
}

// Dunno how to implemente this in terms of operator<<
string
Edge::printReverse() const
{
    testInvariant();

    stringstream os;

    os << "EDGE (rev)";

    os << " label:" << label
       << " depthDelta:" << depthDelta
       << ":" << std::endl
       << "  LINESTRING(";
    auto npts = getNumPoints();
    for(auto i = npts; i > 0; --i) {
        if(i < npts) {
            os << ", ";
        }
        os << pts->getAt(i - 1).toString();
    }
    os << ")";

    return os.str();
}

const Envelope*
Edge::getEnvelope()
{
    return &env;
}

std::ostream&
operator<< (std::ostream& os, const Edge& e)
{
    os << "edge";

    os
            << "  LINESTRING"
            << *(e.pts)
            << "  " << e.label
            << "  " << e.depthDelta
            ;

    return os;
}

} // namespace geos.geomgraph
} // namespace geos

