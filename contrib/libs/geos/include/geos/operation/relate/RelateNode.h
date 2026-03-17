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
 * Last port: operation/relate/RelateNode.java rev. 1.11 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_RELATE_RELATENODE_H
#define GEOS_OP_RELATE_RELATENODE_H

#include <geos/export.h>

#include <geos/geomgraph/Node.h> // for inheritance

// Forward declarations
namespace geos {
namespace geom {
class IntersectionMatrix;
class Coordinate;
}
namespace geomgraph {
class EdgeEndStar;
}
}


namespace geos {
namespace operation { // geos::operation
namespace relate { // geos::operation::relate

/** \brief
 * Represents a node in the topological graph used to compute spatial
 * relationships.
 */
class GEOS_DLL RelateNode: public geomgraph::Node {

public:

    RelateNode(const geom::Coordinate& coord, geomgraph::EdgeEndStar* edges);

    ~RelateNode() override = default;

    /**
     * Update the IM with the contribution for the EdgeEnds incident on this node.
     */
    void updateIMFromEdges(geom::IntersectionMatrix& im);

protected:

    void computeIM(geom::IntersectionMatrix& im) override;
};


} // namespace geos:operation:relate
} // namespace geos:operation
} // namespace geos

#endif // GEOS_OP_RELATE_RELATENODE_H
