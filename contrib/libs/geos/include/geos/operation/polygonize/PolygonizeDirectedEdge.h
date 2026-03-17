/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
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


#ifndef GEOS_OP_POLYGONIZE_POLYGONIZEDIRECTEDEDGE_H
#define GEOS_OP_POLYGONIZE_POLYGONIZEDIRECTEDEDGE_H

#include <geos/export.h>

#include <geos/planargraph/DirectedEdge.h> // for inheritance

// Forward declarations
namespace geos {
namespace geom {
//class LineString;
}
namespace planargraph {
class Node;
}
namespace operation {
namespace polygonize {
class EdgeRing;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace polygonize { // geos::operation::polygonize

/** \brief
 * A DirectedEdge of a PolygonizeGraph, which represents
 * an edge of a polygon formed by the graph.
 *
 * May be logically deleted from the graph by setting the
 * <code>marked</code> flag.
 */
class GEOS_DLL PolygonizeDirectedEdge: public planargraph::DirectedEdge {

private:

    EdgeRing* edgeRing;

    PolygonizeDirectedEdge* next;

    long label;

public:

    /*
     * \brief
     * Constructs a directed edge connecting the <code>from</code> node
     * to the <code>to</code> node.
     *
     * @param directionPt
     *    specifies this DirectedEdge's direction (given by an imaginary
     *    line from the <code>from</code> node to <code>directionPt</code>)
     *
     * @param edgeDirection
     *    whether this DirectedEdge's direction is the same as or
     *    opposite to that of the parent Edge (if any)
     */
    PolygonizeDirectedEdge(planargraph::Node* newFrom,
                           planargraph::Node* newTo,
                           const geom::Coordinate& newDirectionPt,
                           bool nEdgeDirection);

    /*
     * Returns the identifier attached to this directed edge.
     */
    long getLabel() const;

    /*
     * Attaches an identifier to this directed edge.
     */
    void setLabel(long newLabel);

    /*
     * Returns the next directed edge in the EdgeRing that this
     * directed edge is a member of.
     */
    PolygonizeDirectedEdge* getNext() const;

    /*
     * Gets the EdgeRing this edge is a member of.
     */
    EdgeRing* getRing() const {
        return edgeRing;
    }

    /*
     * Sets the next directed edge in the EdgeRing that this
     * directed edge is a member of.
     */
    void setNext(PolygonizeDirectedEdge* newNext);

    /*
     * Returns the ring of directed edges that this directed edge is
     * a member of, or null if the ring has not been set.
     * @see #setRing(EdgeRing)
     */
    bool isInRing() const;

    /*
     * Sets the ring of directed edges that this directed edge is
     * a member of.
     */
    void setRing(EdgeRing* newEdgeRing);
};
} // namespace geos::operation::polygonize
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_POLYGONIZE_POLYGONIZEDIRECTEDEDGE_H
