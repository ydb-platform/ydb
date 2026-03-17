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

#ifndef GEOS_PLANARGRAPH_EDGE_H
#define GEOS_PLANARGRAPH_EDGE_H

#include <geos/export.h>

#include <geos/planargraph/GraphComponent.h> // for inheritance

#include <vector> // for typedefs
#include <set> // for typedefs
#include <iosfwd> // ostream

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace planargraph {
class DirectedEdgeStar;
class DirectedEdge;
class Edge;
class Node;
}
}

namespace geos {
namespace planargraph { // geos.planargraph

/**
 * \brief Represents an undirected edge of a PlanarGraph.
 *
 * An undirected edge in fact simply acts as a central point of reference
 * for two opposite DirectedEdge.
 *
 * Usually a client using a PlanarGraph will subclass Edge
 * to add its own application-specific data and methods.
 */
class GEOS_DLL Edge: public GraphComponent {

public:

    friend std::ostream& operator<< (std::ostream& os, const Node&);

    /// Set of const Edges pointers
    typedef std::set<const Edge*> ConstSet;

    /// Set of non-const Edges pointers
    typedef std::set<Edge*> NonConstSet;

    /// Vector of non-const Edges pointers
    typedef std::vector<Edge*> NonConstVect;

    /// Vector of const Edges pointers
    typedef std::vector<const Edge*> ConstVect;

protected:

    /** \brief The two DirectedEdges associated with this Edge */
    std::vector<DirectedEdge*> dirEdge;

public:

    /** \brief
     * Constructs a Edge whose DirectedEdges are not yet set.
     *
     * Be sure to call
     * {@link #setDirectedEdges(DirectedEdge* de0, DirectedEdge* de1)}
     */
    Edge(): dirEdge() {}

    /**
     * \brief Constructs an Edge initialized with the given DirectedEdges.
     *
     * For each DirectedEdge: sets the Edge, sets the symmetric
     * DirectedEdge, and adds this Edge to its from-Node.
     */
    Edge(DirectedEdge* de0, DirectedEdge* de1)
        :
        dirEdge()
    {
        setDirectedEdges(de0, de1);
    }

    /**
     * \brief Initializes this Edge's two DirectedEdges.
     *
     * For each DirectedEdge:
     * - sets the Edge, sets the symmetric DirectedEdge, and
     * - adds this Edge to its from-Node.
     */
    void setDirectedEdges(DirectedEdge* de0, DirectedEdge* de1);

    /**
     * \brief Returns one of the DirectedEdges associated with this Edge.
     * @param i 0 or 1
     */
    DirectedEdge* getDirEdge(int i);

    /**
     * \brief Returns the DirectedEdge that starts from the given node,
     * or null if the node is not one of the two nodes associated
     * with this Edge.
     */
    DirectedEdge* getDirEdge(Node* fromNode);

    /**
     * \brief If <code>node</code> is one of the two nodes associated
     * with this Edge, returns the other node; otherwise returns null.
     */
    Node* getOppositeNode(Node* node);
};

/// Print a Edge
std::ostream& operator<<(std::ostream& os, const Edge& n);

/// For backward compatibility
//typedef Edge planarEdge;

} // namespace geos::planargraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_PLANARGRAPH_EDGE_H
