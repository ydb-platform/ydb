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
 *
 **********************************************************************
 *
 * Last port: noding/MCIndexSegmentSetMutualIntersector.java r388 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_NODING_MCINDEXSEGMENTSETMUTUALINTERSECTOR_H
#define GEOS_NODING_MCINDEXSEGMENTSETMUTUALINTERSECTOR_H

#include <geos/noding/SegmentSetMutualIntersector.h> // inherited
#include <geos/index/chain/MonotoneChainOverlapAction.h> // inherited

namespace geos {
namespace index {
class SpatialIndex;

namespace chain {
class MonotoneChain;
}
namespace strtree {
//class STRtree;
}
}
namespace noding {
class SegmentString;
class SegmentIntersector;
}
}

//using namespace geos::index::strtree;

namespace geos {
namespace noding { // geos::noding

/** \brief
 * Intersects two sets of [SegmentStrings](@ref SegmentString) using a index based
 * on [MonotoneChains](@ref index::chain::MonotoneChain) and a
 * [SpatialIndex](@ref index::SpatialIndex).
 *
 * @version 1.7
 */
class MCIndexSegmentSetMutualIntersector : public SegmentSetMutualIntersector {
public:

    MCIndexSegmentSetMutualIntersector();

    ~MCIndexSegmentSetMutualIntersector() override;

    index::SpatialIndex*
    getIndex()
    {
        return index;
    }

    void setBaseSegments(SegmentString::ConstVect* segStrings) override;

    // NOTE: re-populates the MonotoneChain vector with newly created chains
    void process(SegmentString::ConstVect* segStrings) override;

    class SegmentOverlapAction : public index::chain::MonotoneChainOverlapAction {
    private:
        SegmentIntersector& si;

        // Declare type as noncopyable
        SegmentOverlapAction(const SegmentOverlapAction& other) = delete;
        SegmentOverlapAction& operator=(const SegmentOverlapAction& rhs) = delete;

    public:
        SegmentOverlapAction(SegmentIntersector& p_si) :
            index::chain::MonotoneChainOverlapAction(), si(p_si)
        {}

        void overlap(index::chain::MonotoneChain& mc1, std::size_t start1,
                     index::chain::MonotoneChain& mc2, std::size_t start2) override;
    };

    /**
     * Disable copy construction and assignment. Apparently needed to make this
     * class compile under MSVC. (See https://stackoverflow.com/q/29565299)
     */
    MCIndexSegmentSetMutualIntersector(const MCIndexSegmentSetMutualIntersector&) = delete;
    MCIndexSegmentSetMutualIntersector& operator=(const MCIndexSegmentSetMutualIntersector&) = delete;

private:

    typedef std::vector<std::unique_ptr<index::chain::MonotoneChain>> MonoChains;
    MonoChains monoChains;

    /*
     * The index::SpatialIndex used should be something that supports
     * envelope (range) queries efficiently (such as a index::quadtree::Quadtree
     * or index::strtree::STRtree).
     */
    index::SpatialIndex* index;
    int indexCounter;
    int processCounter;
    // statistics
    int nOverlaps;

    /* memory management helper, holds MonotoneChain objects used
     * in the SpatialIndex. It's cleared when the SpatialIndex is
     */
    MonoChains chainStore;

    void addToIndex(SegmentString* segStr);

    void intersectChains();

    void addToMonoChains(SegmentString* segStr);

};

} // namespace geos::noding
} // namespace geos

#endif // GEOS_NODING_MCINDEXSEGMENTSETMUTUALINTERSECTOR_H
