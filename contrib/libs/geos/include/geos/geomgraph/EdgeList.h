/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: geomgraph/EdgeList.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_EDGELIST_H
#define GEOS_GEOMGRAPH_EDGELIST_H

#include <geos/export.h>
#include <vector>
#include <unordered_map>
#include <string>
#include <iostream>

#include <geos/noding/OrientedCoordinateArray.h> // for map comparator

#include <geos/inline.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace index {
class SpatialIndex;
}
namespace geomgraph {
class Edge;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

/** \brief
 * A EdgeList is a list of Edges.
 *
 * It supports locating edges
 * that are pointwise equals to a target edge.
 */
class GEOS_DLL EdgeList {

private:

    std::vector<Edge*> edges;

    struct OcaCmp {
        bool
        operator()(
            const noding::OrientedCoordinateArray* oca1,
            const noding::OrientedCoordinateArray* oca2) const
        {
            return *oca1 < *oca2;
        }
    };

    /**
     * An index of the edges, for fast lookup.
     */
    typedef std::unordered_map<noding::OrientedCoordinateArray,
                               Edge*,
                               noding::OrientedCoordinateArray::HashCode> EdgeMap;
    EdgeMap ocaMap;

public:
    friend std::ostream& operator<< (std::ostream& os, const EdgeList& el);

    EdgeList()
        :
        edges(),
        ocaMap()
    {}

    virtual ~EdgeList() = default;

    /**
     * Insert an edge unless it is already in the list
     */
    void add(Edge* e);

    void addAll(const std::vector<Edge*>& edgeColl);

    std::vector<Edge*>&
    getEdges()
    {
        return edges;
    }

    Edge* findEqualEdge(const Edge* e) const;

    Edge* get(int i);

    int findEdgeIndex(const Edge* e) const;

    std::string print();

    void clearList();

};

std::ostream& operator<< (std::ostream& os, const EdgeList& el);


} // namespace geos.geomgraph
} // namespace geos

//#ifdef GEOS_INLINE
//# include "geos/geomgraph/EdgeList.inl"
//#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ifndef GEOS_GEOMGRAPH_EDGELIST_H
