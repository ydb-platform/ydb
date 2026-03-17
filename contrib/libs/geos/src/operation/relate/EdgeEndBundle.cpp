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
 * Last port: operation/relate/EdgeEndBundle.java rev. 1.17 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/relate/EdgeEndBundle.h>
#include <geos/geom/Location.h>
#include <geos/geomgraph/Label.h>
#include <geos/geomgraph/EdgeEnd.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/GeometryGraph.h>
#include <geos/geom/Position.h>

#include <vector>

using namespace std;
using namespace geos::geomgraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace relate { // geos.operation.relate

EdgeEndBundle::EdgeEndBundle(EdgeEnd* e):
    EdgeEnd(e->getEdge(), e->getCoordinate(),
            e->getDirectedCoordinate(),
            e->getLabel())
{
    insert(e);
}

EdgeEndBundle::~EdgeEndBundle()
{
    for(size_t i = 0, n = edgeEnds.size(); i < n; i++) {
        delete edgeEnds[i];
    }
}

//Not needed
//public Iterator iterator() { return edgeEnds.iterator(); }

const vector<EdgeEnd*>&
EdgeEndBundle::getEdgeEnds()
{
    return edgeEnds;
}

void
EdgeEndBundle::insert(EdgeEnd* e)
{
    // Assert: start point is the same
    // Assert: direction is the same
    edgeEnds.push_back(e);
}


/**
 * This computes the overall edge label for the set of
 * edges in this EdgeStubBundle.  It essentially merges
 * the ON and side labels for each edge.  These labels must be compatible
 */
void
EdgeEndBundle::computeLabel(
    const algorithm::BoundaryNodeRule& boundaryNodeRule)
{
    // create the label.  If any of the edges belong to areas,
    // the label must be an area label
    bool isArea = false;

    for(EdgeEnd* e: edgeEnds) {
        if(e->getLabel().isArea()) {
            isArea = true;
        }
    }
    if(isArea) {
        label = Label(Location::NONE, Location::NONE, Location::NONE);
    }
    else {
        label = Label(Location::NONE);
    }
    // compute the On label, and the side labels if present
    for(int i = 0; i < 2; i++) {
        computeLabelOn(i, boundaryNodeRule);
        if(isArea) {
            computeLabelSides(i);
        }
    }
}


void
EdgeEndBundle::computeLabelOn(int geomIndex, const algorithm::BoundaryNodeRule& boundaryNodeRule)
{
    // compute the ON location value
    int boundaryCount = 0;
    bool foundInterior = false;

    for(EdgeEnd* e: edgeEnds) {
        Location loc = e->getLabel().getLocation(geomIndex);
        if(loc == Location::BOUNDARY) {
            boundaryCount++;
        }
        if(loc == Location::INTERIOR) {
            foundInterior = true;
        }
    }
    Location loc = Location::NONE;
    if(foundInterior) {
        loc = Location::INTERIOR;
    }
    if(boundaryCount > 0) {
        loc = GeometryGraph::determineBoundary(boundaryNodeRule,
                                               boundaryCount);
    }
    label.setLocation(geomIndex, loc);
}


/**
 * Compute the labelling for each side
 */
void
EdgeEndBundle::computeLabelSides(int geomIndex)
{
    computeLabelSide(geomIndex, Position::LEFT);
    computeLabelSide(geomIndex, Position::RIGHT);
}

/**
 * To compute the summary label for a side, the algorithm is:
 *   FOR all edges
 *     IF any edge's location is INTERIOR for the side, side location = INTERIOR
 *     ELSE IF there is at least one EXTERIOR attribute, side location = EXTERIOR
 *     ELSE  side location = NULL
 *  <br>
 *  Note that it is possible for two sides to have apparently contradictory information
 *  i.e. one edge side may indicate that it is in the interior of a geometry, while
 *  another edge side may indicate the exterior of the same geometry.  This is
 *  not an incompatibility - GeometryCollections may contain two Polygons that touch
 *  along an edge.  This is the reason for Interior-primacy rule above - it
 *  results in the summary label having the Geometry interior on <b>both</b> sides.
 */
void
EdgeEndBundle::computeLabelSide(int geomIndex, int side)
{
    for(EdgeEnd* e: edgeEnds) {
        if(e->getLabel().isArea()) {
            Location loc = e->getLabel().getLocation(geomIndex, side);
            if(loc == Location::INTERIOR) {
                label.setLocation(geomIndex, side, Location::INTERIOR);
                return;
            }
            else if(loc == Location::EXTERIOR) {
                label.setLocation(geomIndex, side, Location::EXTERIOR);
            }
        }
    }
}

void
EdgeEndBundle::updateIM(IntersectionMatrix& im)
{
    Edge::updateIM(label, im);
}

string
EdgeEndBundle::print() const
{
    string out = "EdgeEndBundle--> Label: " + label.toString() + "\n";
    for(auto& e : edgeEnds) {
        out += e->print();
        out += "\n";
    }
    return out;
}

} // namespace geos.operation.relate
} // namespace geos.operation
} // namespace geos
