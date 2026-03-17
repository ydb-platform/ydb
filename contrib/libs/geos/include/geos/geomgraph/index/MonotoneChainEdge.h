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

#ifndef GEOS_GEOMGRAPH_INDEX_MONOTONECHAINEDGE_H
#define GEOS_GEOMGRAPH_INDEX_MONOTONECHAINEDGE_H

#include <geos/export.h>
#include <geos/geom/Envelope.h> // for composition

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
}
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

/// \brief MonotoneChains are a way of partitioning the segments of an edge to
/// allow for fast searching of intersections.
class GEOS_DLL MonotoneChainEdge {
public:
    //MonotoneChainEdge();
    ~MonotoneChainEdge() = default;
    MonotoneChainEdge(Edge* newE);
    const geom::CoordinateSequence* getCoordinates();
    std::vector<size_t>& getStartIndexes();
    double getMinX(size_t chainIndex);
    double getMaxX(size_t chainIndex);

    void computeIntersects(const MonotoneChainEdge& mce,
                           SegmentIntersector& si);

    void computeIntersectsForChain(size_t chainIndex0,
                                   const MonotoneChainEdge& mce, size_t chainIndex1,
                                   SegmentIntersector& si);

protected:
    Edge* e;
    const geom::CoordinateSequence* pts; // cache a reference to the coord array, for efficiency
    // the lists of start/end indexes of the monotone chains.
    // Includes the end point of the edge as a sentinel
    std::vector<size_t> startIndex;
    // these envelopes are created once and reused
private:
    void computeIntersectsForChain(size_t start0, size_t end0,
                                   const MonotoneChainEdge& mce,
                                   size_t start1, size_t end1,
                                   SegmentIntersector& ei);

    bool overlaps(size_t start0, size_t end0, const MonotoneChainEdge& mce, size_t start1, size_t end1);

};

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif

