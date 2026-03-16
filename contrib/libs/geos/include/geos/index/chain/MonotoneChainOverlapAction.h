/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/chain/MonotoneChainOverlapAction.java rev. 1.6 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IDX_CHAIN_MONOTONECHAINOVERLAPACTION_H
#define GEOS_IDX_CHAIN_MONOTONECHAINOVERLAPACTION_H

#include <geos/export.h>
#include <geos/geom/LineSegment.h>


// Forward declarations
namespace geos {
namespace index {
namespace chain {
class MonotoneChain;
}
}
}

namespace geos {
namespace index { // geos::index
namespace chain { // geos::index::chain

/** \brief
 * The action for the internal iterator for performing
 * overlap queries on a MonotoneChain.
 */
class GEOS_DLL MonotoneChainOverlapAction {

protected:

    geom::LineSegment overlapSeg1;

    geom::LineSegment overlapSeg2;

public:

    MonotoneChainOverlapAction() {}

    virtual
    ~MonotoneChainOverlapAction() {}

    /** \brief
     * This function can be overridden if the original chains are needed.
     *
     * @param mc1 a MonotoneChain
     * @param mc2 a MonotoneChain
     * @param start1 the index of the start of the overlapping segment
     *               from mc1
     * @param start2 the index of the start of the overlapping segment
     *               from mc2
     */
    virtual void overlap(MonotoneChain& mc1, std::size_t start1,
                         MonotoneChain& mc2, std::size_t start2);

    /** \brief
     * This is a convenience function which can be overridden to
     * obtain the actual line segments which overlap.
     *
     * **param** `seg1`
     * **param** `seg2`
     */
    virtual void
    overlap(const geom::LineSegment& /*seg1*/,
            const geom::LineSegment& /*seg2*/)
    {}

};

} // namespace geos::index::chain
} // namespace geos::index
} // namespace geos

#endif // GEOS_IDX_CHAIN_MONOTONECHAINOVERLAPACTION_H

