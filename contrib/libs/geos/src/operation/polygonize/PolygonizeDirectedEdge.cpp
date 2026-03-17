/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/polygonize/PolygonizeDirectedEdge.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/polygonize/PolygonizeDirectedEdge.h>
#include <geos/planargraph/DirectedEdge.h>

using namespace geos::planargraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace polygonize { // geos.operation.polygonize

/**
 * Constructs a directed edge connecting the <code>from</code> node to the
 * <code>to</code> node.
 *
 * @param newDirectionPt
 *        specifies this DirectedEdge's direction (given by an imaginary
 *        line from the <code>from</code> node to <code>directionPt</code>)
 *
 * @param nEdgeDirection
 *        whether this DirectedEdge's direction is the same as or
 *        opposite to that of the parent Edge (if any)
 */
PolygonizeDirectedEdge::PolygonizeDirectedEdge(Node* newFrom,
        Node* newTo, const Coordinate& newDirectionPt,
        bool nEdgeDirection)
    :
    DirectedEdge(newFrom, newTo,
                 newDirectionPt, nEdgeDirection)
{
    edgeRing = nullptr;
    next = nullptr;
    label = -1;
}

/*
 * Returns the identifier attached to this directed edge.
 */
long
PolygonizeDirectedEdge::getLabel() const
{
    return label;
}

/*
 * Attaches an identifier to this directed edge.
 */
void
PolygonizeDirectedEdge::setLabel(long newLabel)
{
    label = newLabel;
}

/*
 * Returns the next directed edge in the EdgeRing that this directed
 * edge is a member of.
 */
PolygonizeDirectedEdge*
PolygonizeDirectedEdge::getNext() const
{
    return next;
}

/*
 * Sets the next directed edge in the EdgeRing that this directed
 * edge is a member of.
 */
void
PolygonizeDirectedEdge::setNext(PolygonizeDirectedEdge* newNext)
{
    next = newNext;
}

/*
 * Returns the ring of directed edges that this directed edge is
 * a member of, or null if the ring has not been set.
 * @see #setRing(EdgeRing)
 */
bool
PolygonizeDirectedEdge::isInRing() const
{
    return edgeRing != nullptr;
}

/*
 * Sets the ring of directed edges that this directed edge is
 * a member of.
 */
void
PolygonizeDirectedEdge::setRing(EdgeRing* newEdgeRing)
{
    edgeRing = newEdgeRing;
}

} // namespace geos.operation.polygonize
} // namespace geos.operation
} // namespace geos
