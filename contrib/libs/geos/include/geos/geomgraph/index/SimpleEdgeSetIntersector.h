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

#ifndef GEOS_GEOMGRAPH_INDEX_SIMPLEEDGESETINTERSECTOR_H
#define GEOS_GEOMGRAPH_INDEX_SIMPLEEDGESETINTERSECTOR_H

#include <geos/export.h>
#include <vector>

#include <geos/geomgraph/index/EdgeSetIntersector.h> // for inheritance

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

/// \brief Finds all intersections in one or two sets of edges, using the
/// straightforward method of comparing all segments.
///
/// \note This algorithm is too slow for production use, but is useful for
/// testing purposes.
class GEOS_DLL SimpleEdgeSetIntersector: public EdgeSetIntersector {

public:

    SimpleEdgeSetIntersector();

    void computeIntersections(std::vector<Edge*>* edges,
                              SegmentIntersector* si, bool testAllSegments) override;

    void computeIntersections(std::vector<Edge*>* edges0,
                              std::vector<Edge*>* edges1, SegmentIntersector* si) override;

private:

    int nOverlaps;

    void computeIntersects(Edge* e0, Edge* e1, SegmentIntersector* si);
};

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#endif // GEOS_GEOMGRAPH_INDEX_SIMPLEEDGESETINTERSECTOR_H

