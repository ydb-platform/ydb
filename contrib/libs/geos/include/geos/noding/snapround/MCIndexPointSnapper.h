/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/snapround/MCIndexPointSnapper.java r486 (JTS-1.12+)
 *
 **********************************************************************/

#ifndef GEOS_NODING_SNAPROUND_MCINDEXPOINTSNAPPER_H
#define GEOS_NODING_SNAPROUND_MCINDEXPOINTSNAPPER_H

#include <cstddef>
#include <geos/export.h>

#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geom {
class Envelope;
}
namespace index {
class SpatialIndex;
}
namespace noding {
class SegmentString;
namespace snapround {
class HotPixel;
}
}
}

namespace geos {
namespace noding { // geos::noding
namespace snapround { // geos::noding::snapround

/** \brief
 * "Snaps" all [SegmentStrings](@ref SegmentString) in a [SpatialIndex](@ref index::SpatialIndex) containing
 * [MonotoneChains](@ref index::chain::MonotoneChain) to a given HotPixel.
 *
 */
class GEOS_DLL MCIndexPointSnapper {

public:


    MCIndexPointSnapper(index::SpatialIndex& nIndex)
        :
        index(nIndex)
    {}

    /** \brief
     * Snaps (nodes) all interacting segments to this hot pixel.
     *
     * The hot pixel may represent a vertex of an edge, in which case this
     * routine uses the optimization of not noding the vertex itself.
     *
     * @param hotPixel the hot pixel to snap to
     * @param parentEdge the edge containing the vertex,
     *                   if applicable, or `null`
     * @param vertexIndex the index of the vertex, if applicable, or -1
     * @return `true` if a node was added for this pixel
     */
    bool snap(HotPixel& hotPixel, SegmentString* parentEdge,
              size_t vertexIndex);

    bool
    snap(HotPixel& hotPixel)
    {
        return snap(hotPixel, nullptr, 0);
    }

    geom::Envelope getSafeEnvelope(const HotPixel& hp) const;


private:

    static constexpr double SAFE_ENV_EXPANSION_FACTOR = 0.75;

    index::SpatialIndex& index;

    // Declare type as noncopyable
    MCIndexPointSnapper(const MCIndexPointSnapper& other) = delete;
    MCIndexPointSnapper& operator=(const MCIndexPointSnapper& rhs) = delete;
};


} // namespace geos::noding::snapround
} // namespace geos::noding
} // namespace geos

#endif // GEOS_NODING_SNAPROUND_MCINDEXPOINTSNAPPER_H
