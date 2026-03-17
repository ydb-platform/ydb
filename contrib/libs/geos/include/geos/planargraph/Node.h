/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_PLANARGRAPH_NODE_H
#define GEOS_PLANARGRAPH_NODE_H

#include <geos/export.h>

#include <geos/planargraph/GraphComponent.h> // for inheritance
#include <geos/planargraph/DirectedEdgeStar.h> // for inlines
#include <geos/geom/Coordinate.h> // for composition

// Forward declarations
namespace geos {
namespace planargraph {
//class DirectedEdgeStar;
class DirectedEdge;
}
}

namespace geos {
namespace planargraph { // geos.planargraph

/**
 * \brief A node in a PlanarGraph is a location where 0 or more Edge meet.
 *
 * A node is connected to each of its incident Edges via an outgoing
 * DirectedEdge. Some clients using a <code>PlanarGraph</code> may want to
 * subclass <code>Node</code> to add their own application-specific
 * data and methods.
 *
 */
class GEOS_DLL Node: public GraphComponent {
protected:

    /// The location of this Node
    geom::Coordinate pt;

    /// The collection of DirectedEdges that leave this Node
    DirectedEdgeStar* deStar;

public:

    friend std::ostream& operator << (std::ostream& os, const Node&);

    /** \brief
     * Returns all Edges that connect the two nodes (which are
     * assumed to be different).
     *
     * Note: returned vector is newly allocated, ownership to
     * the caller.
     */
    static std::vector<Edge*>* getEdgesBetween(Node* node0,
            Node* node1);

    /// Constructs a Node with the given location.
    Node(const geom::Coordinate& newPt)
        :
        pt(newPt)
    {
        deStar = new DirectedEdgeStar();
    }

    ~Node() override
    {
        delete deStar;
    }

    /**
     * \brief
     * Constructs a Node with the given location and
     * collection of outgoing DirectedEdges.
     * Takes ownership of the given DirectedEdgeStar!!
     */
    Node(geom::Coordinate& newPt, DirectedEdgeStar* newDeStar)
        :
        pt(newPt),
        deStar(newDeStar)
    {}

    /**
     * \brief Returns the location of this Node.
     */
    geom::Coordinate&
    getCoordinate()
    {
        return pt;
    }

    /**
     * \brief Adds an outgoing DirectedEdge to this Node.
     */
    void
    addOutEdge(DirectedEdge* de)
    {
        deStar->add(de);
    }

    /**
     * \brief Returns the collection of DirectedEdges that
     * leave this Node.
     */
    DirectedEdgeStar*
    getOutEdges()
    {
        return deStar;
    }
    const DirectedEdgeStar*
    getOutEdges() const
    {
        return deStar;
    }

    /**
     * \brief Returns the number of edges around this Node.
     */
    size_t
    getDegree() const
    {
        return deStar->getDegree();
    }

    /**
     * \brief Returns the zero-based index of the given Edge,
     * after sorting in ascending order by angle with
     * the positive x-axis.
     */
    int
    getIndex(Edge* edge)
    {
        return deStar->getIndex(edge);
    }

private:

    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

};

/// Print a Node
std::ostream& operator<<(std::ostream& os, const Node& n);


/// For backward compatibility
//typedef Node planarNode;

} // namespace geos::planargraph
} // namespace geos

#endif // GEOS_PLANARGRAPH_NODE_H
