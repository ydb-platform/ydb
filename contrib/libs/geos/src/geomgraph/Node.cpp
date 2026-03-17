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
 * Last port: geomgraph/Node.java r411 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/geom/Coordinate.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/EdgeEndStar.h>
#include <geos/geomgraph/Label.h>
#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geom/Location.h>
#include <geos/util/IllegalArgumentException.h>
#include <geos/util.h>

#include <cmath>
#include <memory>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif
#ifndef COMPUTE_Z
#define COMPUTE_Z 1
#endif

using namespace std;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

/*public*/
Node::Node(const Coordinate& newCoord, EdgeEndStar* newEdges)
    :
    GraphComponent(Label(0, Location::NONE)),
    coord(newCoord),
    edges(newEdges)

{
#if GEOS_DEBUG
    cerr << "[" << this << "] Node::Node(" << newCoord.toString() << ")" << endl;
#endif

#if COMPUTE_Z
    ztot = 0;
    addZ(newCoord.z);
    if(edges) {
        EdgeEndStar::iterator endIt = edges->end();
        for(EdgeEndStar::iterator it = edges->begin(); it != endIt; ++it) {
            EdgeEnd* ee = *it;
            addZ(ee->getCoordinate().z);
        }
    }
#endif // COMPUTE_Z

    testInvariant();
}

/*public*/
Node::~Node()
{
    testInvariant();
#if GEOS_DEBUG
    cerr << "[" << this << "] Node::~Node()" << endl;
#endif
    delete edges;
}

/*public*/
const Coordinate&
Node::getCoordinate() const
{
    testInvariant();
    return coord;
}

/*public*/
EdgeEndStar*
Node::getEdges()
{
    testInvariant();

    return edges;
}

/*public*/
bool
Node::isIsolated() const
{
    testInvariant();

    return (label.getGeometryCount() == 1);
}

/*public*/
bool
Node::isIncidentEdgeInResult() const
{
    testInvariant();

    if(!edges) {
        return false;
    }

    EdgeEndStar::iterator it = edges->begin();
    EdgeEndStar::iterator endIt = edges->end();
    for(; it != endIt; ++it) {
        assert(*it);
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(*it);
        if(de->getEdge()->isInResult()) {
            return true;
        }
    }
    return false;
}

void
Node::add(EdgeEnd* p_e)
{
    std::unique_ptr<EdgeEnd> e(p_e);
    assert(e);
#if GEOS_DEBUG
    cerr << "[" << this << "] Node::add(" << e->print() << ")" << endl;
#endif
    // Assert: start pt of e is equal to node point
    if(! e->getCoordinate().equals2D(coord)) {
        std::stringstream ss;
        ss << "EdgeEnd with coordinate " << e->getCoordinate()
           << " invalid for node " << coord;
        throw util::IllegalArgumentException(ss.str());
    }

    // It seems it's legal for edges to be NULL
    // we'd not be honouring the promise of adding
    // an EdgeEnd in this case, though ...
    assert(edges);
    //if (edges==NULL) return;

    edges->insert(e.release());
    p_e->setNode(this);
#if COMPUTE_Z
    addZ(p_e->getCoordinate().z);
#endif
    testInvariant();
}

/*public*/
void
Node::mergeLabel(const Node& n)
{
    assert(!n.label.isNull());
    mergeLabel(n.label);
    testInvariant();
}

/*public*/
void
Node::mergeLabel(const Label& label2)
{
    for(int i = 0; i < 2; i++) {
        Location loc = computeMergedLocation(label2, i);
        Location thisLoc = label.getLocation(i);
        if(thisLoc == Location::NONE) {
            label.setLocation(i, loc);
        }
    }
    testInvariant();
}

/*public*/
void
Node::setLabel(int argIndex, Location onLocation)
{
    if(label.isNull()) {
        label = Label(argIndex, onLocation);
    }
    else {
        label.setLocation(argIndex, onLocation);
    }

    testInvariant();
}

/*public*/
void
Node::setLabelBoundary(int argIndex)
{
    Location loc = label.getLocation(argIndex);
    // flip the loc
    Location newLoc;
    switch(loc) {
    case Location::BOUNDARY:
        newLoc = Location::INTERIOR;
        break;
    case Location::INTERIOR:
        newLoc = Location::BOUNDARY;
        break;
    default:
        newLoc = Location::BOUNDARY;
        break;
    }
    label.setLocation(argIndex, newLoc);

    testInvariant();
}

/*public*/
Location
Node::computeMergedLocation(const Label& label2, int eltIndex)
{
    Location loc = Location::NONE;
    loc = label.getLocation(eltIndex);
    if(!label2.isNull(eltIndex)) {
        Location nLoc = label2.getLocation(eltIndex);
        if(loc != Location::BOUNDARY) {
            loc = nLoc;
        }
    }

    testInvariant();

    return loc;
}

/*public*/
string
Node::print()
{
    testInvariant();

    ostringstream ss;
    ss << *this;
    return ss.str();
}

/*public*/
void
Node::addZ(double z)
{
#if GEOS_DEBUG
    cerr << "[" << this << "] Node::addZ(" << z << ")";
#endif
    if(std::isnan(z)) {
#if GEOS_DEBUG
        cerr << " skipped" << endl;
#endif
        return;
    }
    if(find(zvals.begin(), zvals.end(), z) != zvals.end()) {
#if GEOS_DEBUG
        cerr << " already stored" << endl;
#endif
        return;
    }
    zvals.push_back(z);
    ztot += z;
    coord.z = ztot / static_cast<double>(zvals.size());
#if GEOS_DEBUG
    cerr << " added " << z << ": [" << ztot << "/" << zvals.size() << "=" << coord.z << "]" << endl;
#endif
}

/*public*/
const vector<double>&
Node::getZ() const
{
    return zvals;
}

std::ostream&
operator<< (std::ostream& os, const Node& node)
{
    os << "Node[" << &node << "]" << std::endl
       << "  POINT(" << node.coord << ")" << std::endl
       << "  lbl: " << node.label;
    return os;
}

} // namespace geos.geomgraph
} // namespace geos

