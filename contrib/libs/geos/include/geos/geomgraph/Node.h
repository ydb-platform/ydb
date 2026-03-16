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


#ifndef GEOS_GEOMGRAPH_NODE_H
#define GEOS_GEOMGRAPH_NODE_H

#include <geos/export.h>
#include <geos/geomgraph/GraphComponent.h> // for inheritance
#include <geos/geom/Coordinate.h> // for member

#ifndef NDEBUG
#include <geos/geomgraph/EdgeEndStar.h> // for testInvariant
#include <geos/geomgraph/EdgeEnd.h> // for testInvariant
#endif // ndef NDEBUG

#include <geos/inline.h>

#include <cassert>
#include <string>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class IntersectionMatrix;
}
namespace geomgraph {
class Node;
class EdgeEndStar;
class EdgeEnd;
class Label;
class NodeFactory;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

/** \brief The node component of a geometry graph. */
class GEOS_DLL Node: public GraphComponent {
    using GraphComponent::setLabel;

public:

    friend std::ostream& operator<< (std::ostream& os, const Node& node);

    Node(const geom::Coordinate& newCoord, EdgeEndStar* newEdges);

    ~Node() override;

    virtual const geom::Coordinate& getCoordinate() const;

    virtual EdgeEndStar* getEdges();

    bool isIsolated() const override;

    /** \brief
     * Add the edge to the list of edges at this node
     */
    virtual void add(EdgeEnd* e);

    virtual void mergeLabel(const Node& n);

    /** \brief
     * To merge labels for two nodes,
     * the merged location for each LabelElement is computed.
     *
     * The location for the corresponding node LabelElement is set
     * to the result, as long as the location is non-null.
     */
    virtual void mergeLabel(const Label& label2);

    virtual void setLabel(int argIndex, geom::Location onLocation);

    /** \brief
     * Updates the label of a node to BOUNDARY,
     * obeying the mod-2 boundaryDetermination rule.
     */
    virtual void setLabelBoundary(int argIndex);

    /**
     * The location for a given eltIndex for a node will be one
     * of { null, INTERIOR, BOUNDARY }.
     * A node may be on both the boundary and the interior of a geometry;
     * in this case, the rule is that the node is considered to be
     * in the boundary.
     * The merged location is the maximum of the two input values.
     */
    virtual geom::Location computeMergedLocation(const Label& label2, int eltIndex);

    virtual std::string print();

    virtual const std::vector<double>& getZ() const;

    virtual void addZ(double);

    /** \brief
     * Tests whether any incident edge is flagged as
     * being in the result.
     *
     * This test can be used to determine if the node is in the result,
     * since if any incident edge is in the result, the node must be in
     * the result as well.
     *
     * @return <code>true</code> if any indicident edge in the in
     *         the result
     */
    virtual bool isIncidentEdgeInResult() const;

protected:

    void testInvariant() const;

    geom::Coordinate coord;

    EdgeEndStar* edges;

    /** \brief
     * Basic nodes do not compute IMs
     */
    void
    computeIM(geom::IntersectionMatrix& /*im*/) override {}

private:

    std::vector<double> zvals;

    double ztot;

};

std::ostream& operator<< (std::ostream& os, const Node& node);

inline void
Node::testInvariant() const
{
#ifndef NDEBUG
    if(edges) {
        // Each EdgeEnd in the star has this Node's
        // coordinate as first coordinate
        for(EdgeEndStar::iterator
                it = edges->begin(), itEnd = edges->end();
                it != itEnd; it++) {
            EdgeEnd* e = *it;
            assert(e);
            assert(e->getCoordinate().equals2D(coord));
        }
    }

#if 0 // We can't rely on numerical stability with FP computations
    // ztot is the sum of doubnle sin zvals vector
    double ztot_check = 0.0;
    for(std::vector<double>::const_iterator
            i = zvals.begin(), e = zvals.end();
            i != e;
            i++) {
        ztot_check += *i;
    }
    assert(ztot_check == ztot);
#endif // 0

#endif
}


} // namespace geos.geomgraph
} // namespace geos

//#ifdef GEOS_INLINE
//# include "geos/geomgraph/Node.inl"
//#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ifndef GEOS_GEOMGRAPH_NODE_H
