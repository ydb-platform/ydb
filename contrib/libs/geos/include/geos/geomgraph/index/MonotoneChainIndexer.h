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

#ifndef GEOS_GEOMGRAPH_INDEX_MONOTONECHAININDEXER_H
#define GEOS_GEOMGRAPH_INDEX_MONOTONECHAININDEXER_H

#include <vector>
#include <geos/export.h>

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
}
}

namespace geos {
namespace geomgraph { // geos::geomgraph
namespace index { // geos::geomgraph::index

/// \brief MonotoneChains are a way of partitioning the segments of an edge to
/// allow for fast searching of intersections.
///
/// Specifically, a sequence of contiguous line segments is a monotone chain
/// iff all the vectors defined by the oriented segments lies in the same
/// quadrant.
///
/// Monotone Chains have the following useful properties:
///
/// - the segments within a monotone chain will never intersect each other
///
/// - the envelope of any contiguous subset of the segments in a monotone chain
///   is simply the envelope of the endpoints of the subset.
///
/// Property 1 means that there is no need to test pairs of segments from
/// within the same monotone chain for intersection. Property 2 allows binary
/// search to be used to find the intersection points of two monotone chains.
/// For many types of real-world data, these properties eliminate a large
/// number of segment comparisons, producing substantial speed gains.
///
/// \note Due to the efficient intersection test, there is no need to limit the
/// size of chains to obtain fast performance.
class GEOS_DLL MonotoneChainIndexer {

public:

    MonotoneChainIndexer() {}

    void getChainStartIndices(const geom::CoordinateSequence*, std::vector<std::size_t>&);

private:

    std::size_t findChainEnd(const geom::CoordinateSequence* pts, std::size_t start);

};

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#endif // GEOS_GEOMGRAPH_INDEX_MONOTONECHAININDEXER_H

