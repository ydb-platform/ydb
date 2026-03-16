/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/planargraph/DirectedEdge.h>
#include <geos/planargraph/Node.h>
#include <geos/geom/Quadrant.h>
#include <geos/algorithm/Orientation.h>

#include <cmath>
#include <sstream>
#include <vector>
#include <typeinfo>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace planargraph {

/*public*/
void
DirectedEdge::toEdges(vector<DirectedEdge*>& dirEdges, vector<Edge*>& edges)
{
    for(size_t i = 0, n = dirEdges.size(); i < n; ++i) {
        edges.push_back(dirEdges[i]->parentEdge);
    }
}

/*public*/
vector<Edge*>*
DirectedEdge::toEdges(vector<DirectedEdge*>& dirEdges)
{
    vector<Edge*>* edges = new vector<Edge*>();
    toEdges(dirEdges, *edges);
    return edges;
}

/*public*/
DirectedEdge::DirectedEdge(Node* newFrom, Node* newTo,
                           const Coordinate& directionPt, bool newEdgeDirection)
{
    from = newFrom;
    to = newTo;
    edgeDirection = newEdgeDirection;
    p0 = from->getCoordinate();
    p1 = directionPt;
    double dx = p1.x - p0.x;
    double dy = p1.y - p0.y;
    quadrant = geom::Quadrant::quadrant(dx, dy);
    angle = atan2(dy, dx);
    //Assert.isTrue(! (dx == 0 && dy == 0), "EdgeEnd with identical endpoints found");
}

/*public*/
Edge*
DirectedEdge::getEdge() const
{
    return parentEdge;
}

/*public*/
void
DirectedEdge::setEdge(Edge* newParentEdge)
{
    parentEdge = newParentEdge;
}

/*public*/
int
DirectedEdge::getQuadrant() const
{
    return quadrant;
}

/*public*/
const Coordinate&
DirectedEdge::getDirectionPt() const
{
    return p1;
}

/*public*/
bool
DirectedEdge::getEdgeDirection() const
{
    return edgeDirection;
}

/*public*/
Node*
DirectedEdge::getFromNode() const
{
    return from;
}

/*public*/
Node*
DirectedEdge::getToNode() const
{
    return to;
}

/*public*/
Coordinate&
DirectedEdge::getCoordinate() const
{
    return from->getCoordinate();
}

/*public*/
double
DirectedEdge::getAngle() const
{
    return angle;
}

/*public*/
DirectedEdge*
DirectedEdge::getSym() const
{
    return sym;
}

/*
 * Sets this DirectedEdge's symmetric DirectedEdge,
 * which runs in the opposite direction.
 */
void
DirectedEdge::setSym(DirectedEdge* newSym)
{
    sym = newSym;
}

/*public*/
int
DirectedEdge::compareTo(const DirectedEdge* de) const
{
    return compareDirection(de);
}

/*public*/
int
DirectedEdge::compareDirection(const DirectedEdge* e) const
{
// if the rays are in different quadrants, determining the ordering is trivial
    if(quadrant > e->quadrant) {
        return 1;
    }
    if(quadrant < e->quadrant) {
        return -1;
    }
    // vectors are in the same quadrant - check relative orientation of direction vectors
    // this is > e if it is CCW of e
    return algorithm::Orientation::index(e->p0, e->p1, p1);
}

/*public*/
string
DirectedEdge::print() const
{
    ostringstream s;
    s << *this;
    return s.str();
}

std::ostream&
operator << (std::ostream& s, const DirectedEdge& de)
{
    s << typeid(de).name() << ": " << de.p0 << " - " << de.p1;
    s << " " << de.quadrant << ":" << de.angle;
    return s;
}

} // namespace planargraph
} // namespace geos

