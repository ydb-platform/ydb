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

#ifndef GEOS_GEOMGRAPH_INDEX_SIMPLESWEEPLINEINTERSECTOR_H
#define GEOS_GEOMGRAPH_INDEX_SIMPLESWEEPLINEINTERSECTOR_H

#include <geos/export.h>
#include <vector>

#include <geos/geomgraph/index/EdgeSetIntersector.h> // for inheritance

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geomgraph {
class Edge;
namespace index {
class SegmentIntersector;
class SweepLineEvent;
}
}
}

namespace geos {
namespace geomgraph { // geos::geomgraph
namespace index { // geos::geomgraph::index

/** \brief
 * Finds all intersections in one or two sets of edges,
 * using a simple x-axis sweepline algorithm.
 *
 * While still O(n^2) in the worst case, this algorithm
 * drastically improves the average-case time.
 */
class GEOS_DLL SimpleSweepLineIntersector: public EdgeSetIntersector {

public:

    SimpleSweepLineIntersector();

    ~SimpleSweepLineIntersector() override;

    void computeIntersections(std::vector<Edge*>* edges,
                              SegmentIntersector* si,
                              bool testAllSegments) override;

    void computeIntersections(std::vector<Edge*>* edges0,
                              std::vector<Edge*>* edges1,
                              SegmentIntersector* si) override;

private:

    void add(std::vector<Edge*>* edges);

    std::vector<SweepLineEvent*> events;

    // statistics information
    int nOverlaps;

    void add(std::vector<Edge*>* edges, void* edgeSet);

    void add(Edge* edge, void* edgeSet);

    void prepareEvents();

    void computeIntersections(SegmentIntersector* si);

    void processOverlaps(size_t start, size_t end, SweepLineEvent* ev0,
                         SegmentIntersector* si);
};

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_GEOMGRAPH_INDEX_SIMPLESWEEPLINEINTERSECTOR_H

