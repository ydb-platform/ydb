/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/linemerge/LineMergeDirectedEdge.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_LINEMERGE_LINEMERGEDIRECTEDEDGE_H
#define GEOS_OP_LINEMERGE_LINEMERGEDIRECTEDEDGE_H

#include <geos/export.h>

#include <geos/planargraph/DirectedEdge.h> // for inheritance

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
//class LineString;
}
namespace planargraph {
class Node;
}
}


namespace geos {
namespace operation { // geos::operation
namespace linemerge { // geos::operation::linemerge

/**
 * \brief
 * A [DirectedEdge](@ref planargraph::DirectedEdge) of a LineMergeGraph.
 *
 */
class GEOS_DLL LineMergeDirectedEdge: public planargraph::DirectedEdge {
public:
    /** \brief
     * Constructs a LineMergeDirectedEdge connecting the `from`
     * node to the `to` node.
     *
     * @param from `from` node
     * @param to `to` node
     * @param directionPt specifies this DirectedEdge's direction (given by an
     *                    imaginary line from the `from` node to
     *                    `directionPt`)
     * @param edgeDirection whether this DirectedEdge's direction is the same as or
     *                      opposite to that of the parent Edge (if any)
     */
    LineMergeDirectedEdge(planargraph::Node* from,
                          planargraph::Node* to,
                          const geom::Coordinate& directionPt,
                          bool edgeDirection);

    /**  \brief
     * Returns the directed edge that starts at this directed edge's end point, or null
     * if there are zero or multiple directed edges starting there.
     * @return the directed edge
     * @return `null` if there are zero or multiple directed edges starting there
     */
    LineMergeDirectedEdge* getNext();
};

} // namespace geos::operation::linemerge
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_LINEMERGE_LINEMERGEDIRECTEDEDGE_H
