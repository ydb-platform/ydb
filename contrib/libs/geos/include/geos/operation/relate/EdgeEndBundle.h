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
 * Last port: operation/relate/EdgeEndBundle.java rev. 1.17 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_RELATE_EDGEENDBUNDLE_H
#define GEOS_OP_RELATE_EDGEENDBUNDLE_H

#include <geos/export.h>

#include <geos/geomgraph/EdgeEnd.h> // for EdgeEndBundle inheritance

#include <string>

// Forward declarations
namespace geos {
namespace algorithm {
class BoundaryNodeRule;
}
namespace geom {
class IntersectionMatrix;
}
}


namespace geos {
namespace operation { // geos::operation
namespace relate { // geos::operation::relate

/** \brief
 * A collection of geomgraph::EdgeEnd objects which
 * originate at the same point and have the same direction.
 */
class GEOS_DLL EdgeEndBundle: public geomgraph::EdgeEnd {
public:
    EdgeEndBundle(geomgraph::EdgeEnd* e);
    ~EdgeEndBundle() override;
    const std::vector<geomgraph::EdgeEnd*>& getEdgeEnds();
    void insert(geomgraph::EdgeEnd* e);

    void computeLabel(const algorithm::BoundaryNodeRule& bnr) override;

    /**
     * \brief
     * Update the IM with the contribution for the computed label for
     * the EdgeStubs.
     */
    void updateIM(geom::IntersectionMatrix& im);

    std::string print() const override;
protected:
    std::vector<geomgraph::EdgeEnd*> edgeEnds;

    /**
     * Compute the overall ON location for the list of EdgeStubs.
     *
     * (This is essentially equivalent to computing the self-overlay of
     * a single Geometry)
     *
     * edgeStubs can be either on the boundary (eg Polygon edge)
     * OR in the interior (e.g. segment of a LineString)
     * of their parent Geometry.
     *
     * In addition, GeometryCollections use a algorithm::BoundaryNodeRule
     * to determine whether a segment is on the boundary or not.
     *
     * Finally, in GeometryCollections it can occur that an edge
     * is both
     * on the boundary and in the interior (e.g. a LineString segment
     * lying on
     * top of a Polygon edge.) In this case the Boundary is
     * given precendence.
     *
     * These observations result in the following rules for computing
     * the ON location:
     *  - if there are an odd number of Bdy edges, the attribute is Bdy
     *  - if there are an even number >= 2 of Bdy edges, the attribute
     *    is Int
     *  - if there are any Int edges, the attribute is Int
     *  - otherwise, the attribute is NULL.
     *
     */
    void computeLabelOn(int geomIndex,
                        const algorithm::BoundaryNodeRule& boundaryNodeRule);

    void computeLabelSides(int geomIndex);
    void computeLabelSide(int geomIndex, int side);
};

} // namespace geos:operation:relate
} // namespace geos:operation
} // namespace geos

#endif // GEOS_OP_RELATE_EDGEENDBUNDLE_H
