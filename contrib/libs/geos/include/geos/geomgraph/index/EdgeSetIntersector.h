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
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_INDEX_EDGESETINTERSECTOR_H
#define GEOS_GEOMGRAPH_INDEX_EDGESETINTERSECTOR_H

#include <geos/export.h>
#include <vector>

// Forward declarations
namespace geos {
namespace geomgraph {
class Edge;
namespace index {
class SegmentIntersector;
}
}
}

namespace geos {
namespace geomgraph { // geos::geomgraph
namespace index { // geos::geomgraph::index

/** \brief
 * An EdgeSetIntersector computes all the intersections between the edges in the set.
 *
 * \note This is derived from a Java interface.
 */
class GEOS_DLL EdgeSetIntersector {
public:
    /** \brief
     * Computes all self-intersections between edges in a set of edges,
     * allowing client to choose whether self-intersections are computed.
     *
     * @param edges a list of edges to test for intersections
     * @param si the SegmentIntersector to use
     * @param testAllSegments true if self-intersections are to be tested as well
     */
    virtual void computeIntersections(std::vector<Edge*>* edges,
                                      SegmentIntersector* si, bool testAllSegments) = 0;

    /** \brief
     * Computes all mutual intersections between two sets of edges
     */
    virtual void computeIntersections(std::vector<Edge*>* edges0,
                                      std::vector<Edge*>* edges1,
                                      SegmentIntersector* si) = 0;

    virtual
    ~EdgeSetIntersector() {}
};


} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#endif

