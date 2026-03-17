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
 ***********************************************************************
 *
 * Last port: operation/overlay/EdgeSetNoder.java rev. 1.12 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_EDGESETNODER_H
#define GEOS_OP_OVERLAY_EDGESETNODER_H

#include <geos/export.h>

#include <vector>

// Forward declarations
namespace geos {
namespace geomgraph {
class Edge;
}
namespace algorithm {
class LineIntersector;
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay

/** \brief
 * Nodes a set of edges.
 *
 * Takes one or more sets of edges and constructs a
 * new set of edges consisting of all the split edges created by
 * noding the input edges together
 */
class GEOS_DLL EdgeSetNoder {
private:
    algorithm::LineIntersector* li;
    std::vector<geomgraph::Edge*>* inputEdges;

    EdgeSetNoder(const EdgeSetNoder&) = delete;
    EdgeSetNoder& operator=(const EdgeSetNoder&) = delete;

public:
    EdgeSetNoder(algorithm::LineIntersector* newLi)
        :
        li(newLi),
        inputEdges(new std::vector<geomgraph::Edge*>())
    {}

    ~EdgeSetNoder()
    {
        delete inputEdges; // TODO: avoid heap allocation
    }

    void addEdges(std::vector<geomgraph::Edge*>* edges);
    std::vector<geomgraph::Edge*>* getNodedEdges();
};


} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#endif // ndef GEOS_OP_OVERLAY_EDGESETNODER_H
