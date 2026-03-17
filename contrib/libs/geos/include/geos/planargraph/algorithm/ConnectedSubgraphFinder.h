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

#ifndef GEOS_PLANARGRAPH_ALGO_CONNECTEDSUBGRAPHFINDER_H
#define GEOS_PLANARGRAPH_ALGO_CONNECTEDSUBGRAPHFINDER_H

#include <geos/export.h>
#include <geos/planargraph/PlanarGraph.h> // for inlines

#include <stack>
#include <vector>

// Forward declarations
namespace geos {
namespace planargraph {
class PlanarGraph;
class Subgraph;
class Node;
}
}

namespace geos {
namespace planargraph { // geos::planargraph
namespace algorithm { // geos::planargraph::algorithm

/** \brief
 * Finds all connected {@link Subgraph}s of a PlanarGraph.
 *
 * <b>Note:</b> uses the <code>isVisited</code> flag on the nodes.
 */
class GEOS_DLL ConnectedSubgraphFinder {
public:

    ConnectedSubgraphFinder(PlanarGraph& newGraph)
        :
        graph(newGraph)
    {}

    /// \brief
    /// Store newly allocated connected Subgraphs into the
    /// given std::vector
    ///
    /// Caller take responsibility in releasing memory associated
    /// with the subgraphs themself.
    ///
    ///
    void getConnectedSubgraphs(std::vector<Subgraph*>& dest);

private:

    PlanarGraph& graph;

    /// Returns a newly allocated Subgraph
    Subgraph* findSubgraph(Node* node);


    /**
     * Adds all nodes and edges reachable from this node to the subgraph.
     * Uses an explicit stack to avoid a large depth of recursion.
     *
     * @param node a node known to be in the subgraph
     */
    void addReachable(Node* node, Subgraph* subgraph);

    /**
     * Adds the argument node and all its out edges to the subgraph.
     * @param node the node to add
     * @param nodeStack the current set of nodes being traversed
     */
    void addEdges(Node* node, std::stack<Node*>& nodeStack,
                  Subgraph* subgraph);

    // Declare type as noncopyable
    ConnectedSubgraphFinder(const ConnectedSubgraphFinder& other) = delete;
    ConnectedSubgraphFinder& operator=(const ConnectedSubgraphFinder& rhs) = delete;
};

} // namespace geos::planargraph::algorithm
} // namespace geos::planargraph
} // namespace geos

#endif // GEOS_PLANARGRAPH_ALGO_CONNECTEDSUBGRAPHFINDER_H

