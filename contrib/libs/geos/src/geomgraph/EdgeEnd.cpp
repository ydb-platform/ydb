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
 * Last port: geomgraph/EdgeEnd.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/geomgraph/EdgeEnd.h>
#include <geos/geomgraph/Node.h> // for assertions
#include <geos/algorithm/Orientation.h>
#include <geos/geomgraph/Label.h>
#include <geos/geom/Quadrant.h>
#include <geos/geom/Coordinate.h>

#include <typeinfo>
#include <cmath>
#include <sstream>
#include <iostream>
#include <string>
#include <cassert>

using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

using namespace geos::algorithm;

/*public*/
EdgeEnd::EdgeEnd()
    :
    edge(nullptr),
    label(),
    node(nullptr),
    dx(0.0),
    dy(0.0),
    quadrant(0)
{
}

/*protected*/
EdgeEnd::EdgeEnd(Edge* newEdge)
    :
    edge(newEdge),
    label(),
    node(nullptr),
    dx(0.0),
    dy(0.0),
    quadrant(0)
{
}

/*public*/
EdgeEnd::EdgeEnd(Edge* newEdge, const Coordinate& newP0,
                 const Coordinate& newP1, const Label& newLabel)
    :
    edge(newEdge),
    label(newLabel),
    node(nullptr),
    dx(0.0),
    dy(0.0),
    quadrant(0)
{
    init(newP0, newP1);
}

/*public*/
EdgeEnd::EdgeEnd(Edge* newEdge, const Coordinate& newP0,
                 const Coordinate& newP1)
    :
    edge(newEdge),
    label(),
    node(nullptr),
    dx(0.0),
    dy(0.0),
    quadrant(0)
{
    init(newP0, newP1);
}

/*public*/
void
EdgeEnd::init(const Coordinate& newP0, const Coordinate& newP1)
{
    p0 = newP0;
    p1 = newP1;
    dx = p1.x - p0.x;
    dy = p1.y - p0.y;
    quadrant = Quadrant::quadrant(dx, dy);

    // "EdgeEnd with identical endpoints found");
    assert(!(dx == 0 && dy == 0));
}

/*public*/
Coordinate&
EdgeEnd::getDirectedCoordinate()
{
    return p1;
}

/*public*/
int
EdgeEnd::getQuadrant()
{
    return quadrant;
}

/*public*/
double
EdgeEnd::getDx()
{
    return dx;
}

/*public*/
double
EdgeEnd::getDy()
{
    return dy;
}

/*public*/
void
EdgeEnd::setNode(Node* newNode)
{
    node = newNode;
    assert(node->getCoordinate().equals2D(p0));
}

/*public*/
Node*
EdgeEnd::getNode()
{
    return node;
}

/*public*/
int
EdgeEnd::compareTo(const EdgeEnd* e) const
{
    return compareDirection(e);
}

/*public*/
int
EdgeEnd::compareDirection(const EdgeEnd* e) const
{
    assert(e);
    if(dx == e->dx && dy == e->dy) {
        return 0;
    }

    // if the rays are in different quadrants,
    // determining the ordering is trivial
    if(quadrant > e->quadrant) {
        return 1;
    }
    if(quadrant < e->quadrant) {
        return -1;
    }

    // vectors are in the same quadrant - check relative
    // orientation of direction vectors
    // this is > e if it is CCW of e
    return Orientation::index(e->p0, e->p1, p1);
}

/*public*/
void
EdgeEnd::computeLabel(const algorithm::BoundaryNodeRule& /*boundaryNodeRule*/)
{
    // subclasses should override this if they are using labels
}

/*public*/
std::string
EdgeEnd::print() const
{
    std::ostringstream s;

    s << *this;

    return s.str();
}

std::ostream&
operator<< (std::ostream& os, const EdgeEnd& ee)
{
    os << "EdgeEnd: ";
    os << ee.p0;
    os << " - ";
    os << ee.p1;
    os << " ";
    os << ee.quadrant << ":" << std::atan2(ee.dy, ee.dx);
    os << "  ";
    os << ee.label;

    return os;
}


} // namespace geos.geomgraph
} // namespace geos

