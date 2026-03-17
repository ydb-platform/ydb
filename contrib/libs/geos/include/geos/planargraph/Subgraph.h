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

#ifndef GEOS_PLANARGRAPH_SUBGRAPH_H
#define GEOS_PLANARGRAPH_SUBGRAPH_H

#include <geos/export.h>
#include <geos/planargraph/NodeMap.h> // for composition

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace planargraph {
class PlanarGraph;
class DirectedEdge;
class Edge;
}
}

namespace geos {
namespace planargraph { // geos.planargraph

/// \brief A subgraph of a PlanarGraph.
///
/// A subgraph may contain any subset of {@link Edge}s
/// from the parent graph.
/// It will also automatically contain all {@link DirectedEdge}s
/// and {@link Node}s associated with those edges.
/// No new objects are created when edges are added -
/// all associated components must already exist in the parent graph.
///
/// @note Actually we'll be copying Coordinates in NodeMap.
/// I guess that'll need to be changed soon.
///
class GEOS_DLL Subgraph {
public:
    /** \brief
     * Creates a new subgraph of the given PlanarGraph
     *
     * @param parent the parent graph
     */
    Subgraph(PlanarGraph& parent)
        :
        parentGraph(parent)
    {}

    /** \brief
     * Gets the PlanarGraph which this subgraph
     * is part of.
     *
     * @return the parent PlanarGraph
     */
    PlanarGraph&
    getParent() const
    {
        return parentGraph;
    }

    /** \brief
     * Adds an Edge to the subgraph.
     *
     * The associated {@link DirectedEdge}s and {@link Node}s
     * are also added.
     *
     * @param e the edge to add
     *
     * @return a pair with first element being an iterator
     *         to the Edge in set and second element
     *	       being a boolean value indicating wheter
     *	       the Edge has been inserted now or was
     *	       already in the set.
     */
    std::pair<std::set<Edge*>::iterator, bool> add(Edge* e);

    /** \brief
     * Returns an iterator over the DirectedEdge in this graph,
     * in the order in which they were added.
     *
     * @return an iterator over the directed edges
     *
     * @see add(Edge)
     */
    std::vector<const DirectedEdge*>::iterator
    getDirEdgeBegin()
    {
        return dirEdges.begin();
    }


    /** \brief
     * Returns an iterator over the {@link Edge}s in this
     * graph, in the order in which they were added.
     *
     * @return an iterator over the edges
     *
     * @see add(Edge)
     */
    std::set<Edge*>::iterator
    edgeBegin()
    {
        return edges.begin();
    }
    std::set<Edge*>::iterator
    edgeEnd()
    {
        return edges.end();
    }

    /** \brief
     * Returns a iterators over the planar NodeMap::container
     * in this graph.
     */
    NodeMap::container::iterator
    nodeBegin()
    {
        return nodeMap.begin();
    }
    NodeMap::container::const_iterator
    nodeEnd() const
    {
        return nodeMap.end();
    }
    NodeMap::container::iterator
    nodeEnd()
    {
        return nodeMap.end();
    }
    NodeMap::container::const_iterator
    nodeBegin() const
    {
        return nodeMap.begin();
    }

    /** \brief
     * Tests whether an {@link Edge} is contained in this subgraph.
     *
     * @param e the edge to test
     * @return `true` if the edge is contained in this subgraph
     */
    bool
    contains(Edge* e)
    {
        return (edges.find(e) != edges.end());
    }

protected:

    PlanarGraph& parentGraph;
    std::set<Edge*> edges;
    std::vector<const DirectedEdge*> dirEdges;
    NodeMap nodeMap;

    // Declare type as noncopyable
    Subgraph(const Subgraph& other) = delete;
    Subgraph& operator=(const Subgraph& rhs) = delete;
};

} // namespace geos::planargraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_PLANARGRAPH_SUBGRAPH_H
