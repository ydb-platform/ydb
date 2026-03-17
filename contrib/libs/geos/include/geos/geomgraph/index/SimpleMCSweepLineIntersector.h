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

#ifndef GEOS_GEOMGRAPH_INDEX_SIMPLEMCSWEEPLINEINTERSECTOR_H
#define GEOS_GEOMGRAPH_INDEX_SIMPLEMCSWEEPLINEINTERSECTOR_H

#include <geos/export.h>
#include <memory>
#include <vector>

#include <geos/geomgraph/index/EdgeSetIntersector.h> // for inheritance
#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/geomgraph/index/SweepLineEvent.h>
#include <geos/geomgraph/index/MonotoneChain.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geomgraph {
class Edge;
}
}

namespace geos {
namespace geomgraph { // geos::geomgraph
namespace index { // geos::geomgraph::index

/** \brief
 * Finds all intersections in one or two sets of edges,
 * using an x-axis sweepline algorithm in conjunction with Monotone Chains.
 *
 * While still O(n^2) in the worst case, this algorithm
 * drastically improves the average-case time.
 * The use of MonotoneChains as the items in the index
 * seems to offer an improvement in performance over a sweep-line alone.
 */
class GEOS_DLL SimpleMCSweepLineIntersector: public EdgeSetIntersector {

public:

    SimpleMCSweepLineIntersector() = default;

    ~SimpleMCSweepLineIntersector() override = default;

    void computeIntersections(std::vector<Edge*>* edges,
                              SegmentIntersector* si, bool testAllSegments) override;

    void computeIntersections(std::vector<Edge*>* edges0,
                              std::vector<Edge*>* edges1,
                              SegmentIntersector* si) override;

protected:

    // SweepLineEvents need to refer to each other, and to MonotoneChains.
    // To avoid individually heap-allocating all of these, we store them
    // in deques so that subsequent inserts preserve addresses. However,
    // we also need to sort the SweepLineEvents after they have all been
    // inserted, so we keep a pointer to each event in a separate vector,
    // which can be freely reordered without breaking linkages.
    std::vector<SweepLineEvent*> events;
    std::deque<SweepLineEvent> eventStore;
    std::deque<MonotoneChain> chains;

    // statistics information
    int nOverlaps;

private:
    void add(std::vector<Edge*>* edges);

    void add(std::vector<Edge*>* edges, void* edgeSet);

    void add(Edge* edge, void* edgeSet);

    void prepareEvents();

    void computeIntersections(SegmentIntersector* si);

    void processOverlaps(size_t start, size_t end,
                         SweepLineEvent* ev0,
                         SegmentIntersector* si);
    // Declare type as noncopyable
    SimpleMCSweepLineIntersector(const SimpleMCSweepLineIntersector& other) = delete;
    SimpleMCSweepLineIntersector& operator=(const SimpleMCSweepLineIntersector& rhs) = delete;
};

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_GEOMGRAPH_INDEX_SIMPLEMCSWEEPLINEINTERSECTOR_H

