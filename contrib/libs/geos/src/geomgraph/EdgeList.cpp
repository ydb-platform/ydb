/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geomgraph/EdgeList.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/

#include <string>
#include <sstream>
#include <vector>

#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/EdgeList.h>
#include <geos/noding/OrientedCoordinateArray.h>
#include <geos/profiler.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace geos::noding;

namespace geos {
namespace geomgraph { // geos.geomgraph

#if PROFILE
static Profiler* profiler = Profiler::instance();
#endif

/*public*/
void
EdgeList::add(Edge* e)
{
    edges.push_back(e);
    OrientedCoordinateArray oca(*e->getCoordinates());
    ocaMap[oca] = e;
}

void
EdgeList::addAll(const std::vector<Edge*>& edgeColl)
{
    for(std::size_t i = 0, s = edgeColl.size(); i < s ; ++i) {
        add(edgeColl[i]);
    }
}

/**
 * If there is an edge equal to e already in the list, return it.
 * Otherwise return null.
 * @return  equal edge, if there is one already in the list
 *          null otherwise
 */
Edge*
EdgeList::findEqualEdge(const Edge* e) const
{
#if PROFILE
    static Profile* prof = profiler->get("EdgeList::findEqualEdge(Edge *e)");
    prof->start();
#endif

    OrientedCoordinateArray oca(*(e->getCoordinates()));

    auto it = ocaMap.find(oca);

#if PROFILE
    prof->stop();
#endif

    if(it != ocaMap.end()) {
        return it->second;
    }
    return nullptr;
}

Edge*
EdgeList::get(int i)
{
    return edges[i];
}

/**
 * If the edge e is already in the list, return its index.
 * @return  index, if e is already in the list
 *          -1 otherwise
 */
int
EdgeList::findEdgeIndex(const Edge* e) const
{
    for(int i = 0, s = (int)edges.size(); i < s; ++i) {
        if(edges[i]->equals(e)) {
            return i;
        }
    }
    return -1;
}

std::string
EdgeList::print()
{
    std::ostringstream ss;
    ss << *this;
    return ss.str();
#if 0
    string out = "EdgeList( ";
    for(unsigned int j = 0, s = edges.size(); j < s; ++j) {
        Edge* e = edges[j];
        if(j) {
            out += ",";
        }
        out += e->print();
    }
    out += ")  ";
    return out;
#endif
}

void
EdgeList::clearList()
{
    for(unsigned int pos = 0; pos < edges.size(); pos++) {
        delete edges[pos];
    }

    edges.clear();
}

std::ostream&
operator<< (std::ostream& os, const EdgeList& el)
{
    os << "EdgeList: " << std::endl;
    for(std::size_t j = 0, s = el.edges.size(); j < s; ++j) {
        Edge* e = el.edges[j];
        os << "  " << *e << std::endl;
    }
    return os;
}

} // namespace geos.geomgraph
} // namespace geos
