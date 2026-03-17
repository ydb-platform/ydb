/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: planargraph/PlanarGraph.java rev. 107/138 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/planargraph/PlanarGraph.h>
#include <geos/planargraph/Edge.h>
#include <geos/planargraph/NodeMap.h>
#include <geos/planargraph/Node.h>
#include <geos/planargraph/DirectedEdge.h>

#include <vector>
#include <map>

using namespace std;

namespace geos {
namespace planargraph {


/*
 * Adds the Edge and its DirectedEdges with this PlanarGraph.
 * Assumes that the Edge has already been created with its associated
 * DirectEdges.
 * Only subclasses can add Edges, to ensure the edges added are of
 * the right class.
 */
void
PlanarGraph::add(Edge* edge)
{
    edges.push_back(edge);
    add(edge->getDirEdge(0));
    add(edge->getDirEdge(1));
}


/*
 * Removes an Edge and its associated DirectedEdges from their from-Nodes and
 * from this PlanarGraph. Note: This method does not remove the Nodes associated
 * with the Edge, even if the removal of the Edge reduces the degree of a
 * Node to zero.
 */
void
PlanarGraph::remove(Edge* edge)
{
    remove(edge->getDirEdge(0));
    remove(edge->getDirEdge(1));
    for(unsigned int i = 0; i < edges.size(); ++i) {
        if(edges[i] == edge) {
            edges.erase(edges.begin() + i);
            --i;
        }
    }
}

/*
 * Removes DirectedEdge from its from-Node and from this PlanarGraph. Note:
 * This method does not remove the Nodes associated with the DirectedEdge,
 * even if the removal of the DirectedEdge reduces the degree of a Node to
 * zero.
 */
void
PlanarGraph::remove(DirectedEdge* de)
{
    DirectedEdge* sym = de->getSym();
    if(sym != nullptr) {
        sym->setSym(nullptr);
    }
    de->getFromNode()->getOutEdges()->remove(de);
    for(unsigned int i = 0; i < dirEdges.size(); ++i) {
        if(dirEdges[i] == de) {
            dirEdges.erase(dirEdges.begin() + i);
            --i;
        }
    }
}

/*
 * Removes a node from the graph, along with any associated
 * DirectedEdges and Edges.
 */
void
PlanarGraph::remove(Node* node)
{
    // unhook all directed edges
    vector<DirectedEdge*>& outEdges = node->getOutEdges()->getEdges();
    for(unsigned int i = 0; i < outEdges.size(); ++i) {
        DirectedEdge* de = outEdges[i];
        DirectedEdge* sym = de->getSym();
        // remove the diredge that points to this node
        if(sym != nullptr) {
            remove(sym);
        }
        // remove this diredge from the graph collection
        for(unsigned int j = 0; j < dirEdges.size(); ++j) {
            if(dirEdges[j] == de) {
                dirEdges.erase(dirEdges.begin() + j);
                --j;
            }
        }
        Edge* edge = de->getEdge();
        if(edge != nullptr) {
            for(unsigned int k = 0; k < edges.size(); ++k) {
                if(edges[k] == edge) {
                    edges.erase(edges.begin() + k);
                    --k;
                }
            }
        }
    }
    // remove the node from the graph
    nodeMap.remove(node->getCoordinate());
    //nodes.remove(node);
}

/*public*/
vector<Node*>*
PlanarGraph::findNodesOfDegree(size_t degree)
{
    vector<Node*>* nodesFound = new vector<Node*>();
    findNodesOfDegree(degree, *nodesFound);
    return nodesFound;
}

/*public*/
void
PlanarGraph::findNodesOfDegree(size_t degree, vector<Node*>& nodesFound)
{
    NodeMap::container& nm = nodeMap.getNodeMap();
    for(NodeMap::container::iterator it = nm.begin(), itEnd = nm.end();
            it != itEnd; ++it) {
        Node* node = it->second;
        if(node->getDegree() == degree) {
            nodesFound.push_back(node);
        }
    }
}

} // namespace planargraph
} // namespace geos

