/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

#include <cassert>
#include <string>
#include <sstream>

#include <geos/edgegraph/HalfEdge.h>
#include <geos/edgegraph/EdgeGraph.h>
#include <geos/geom/Coordinate.h>

using namespace geos::geom;

namespace geos {
namespace edgegraph { // geos.edgegraph

/*protected*/
HalfEdge*
EdgeGraph::createEdge(const Coordinate& orig)
{
    edges.emplace_back(orig);
    return &(edges.back());
}

/*private*/
HalfEdge*
EdgeGraph::create(const Coordinate& p0, const Coordinate& p1)
{
    HalfEdge* e0 = createEdge(p0);
    HalfEdge* e1 = createEdge(p1);
    e0->link(e1);
    return e0;
}

/*public*/
HalfEdge*
EdgeGraph::addEdge(const Coordinate& orig, const Coordinate& dest)
{
    if (! isValidEdge(orig, dest)) {
        return nullptr;
    }

    /**
     * Attempt to find the edge already in the graph.
     * Return it if found.
     * Otherwise, use a found edge with same origin (if any) to construct new edge.
     */
    HalfEdge* eAdj = nullptr;
    auto it = vertexMap.find(orig);
    if (it != vertexMap.end()) {
        eAdj = it->second;
    }

    HalfEdge* eSame = nullptr;
    if (eAdj != nullptr) {
        eSame = eAdj->find(dest);
    }
    if (eSame != nullptr) {
        return eSame;
    }

    HalfEdge* e = insert(orig, dest, eAdj);
    return e;
}

/*public static*/
bool
EdgeGraph::isValidEdge(const Coordinate& orig, const Coordinate& dest)
{
    return dest.compareTo(orig) != 0;
}

/*private*/
HalfEdge*
EdgeGraph::insert(const Coordinate& orig, const Coordinate& dest, HalfEdge* eAdj)
{
    // edge does not exist, so create it and insert in graph
    HalfEdge* e = create(orig, dest);
    if (eAdj != nullptr) {
        eAdj->insert(e);
    }
    else {
        // add halfedges to to map
        vertexMap[orig] = e;
    }

    HalfEdge* eAdjDest = nullptr;
    auto it = vertexMap.find(dest);
    if (it != vertexMap.end()) {
        eAdjDest = it->second;
    }
    if (eAdjDest != nullptr) {
        eAdjDest->insert(e->sym());
    }
    else {
        vertexMap[dest] = e->sym();
    }
    return e;
}

/*public*/
void
EdgeGraph::getVertexEdges(std::vector<const HalfEdge*>& edgesOut)
{
    for (auto it = vertexMap.begin(); it != vertexMap.end(); ++it) {
        edgesOut.push_back(it->second);
    }
    return;
}

/*public*/
HalfEdge*
EdgeGraph::findEdge(const Coordinate& orig, const Coordinate& dest)
{
    HalfEdge* e = nullptr;
    auto it = vertexMap.find(orig);
    if (it != vertexMap.end()) {
        e = it->second;
    }
    if (e == nullptr) {
        return nullptr;
    }
    return e->find(dest);
}



} // namespace geos.edgegraph
} // namespace geos
