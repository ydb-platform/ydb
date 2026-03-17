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
 **********************************************************************
 *
 * Last port: geomgraph/index/MonotoneChain.java rev. 1.3 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_INDEX_MONOTONECHAIN_H
#define GEOS_GEOMGRAPH_INDEX_MONOTONECHAIN_H


#include <geos/export.h>
#include <geos/geomgraph/index/SweepLineEventObj.h> // for inheritance
#include <geos/geomgraph/index/MonotoneChainEdge.h> // for inline
#include <geos/geomgraph/index/MonotoneChain.h>

// Forward declarations
namespace geos {
namespace geomgraph {
namespace index {
class SegmentIntersector;
}
}
}

namespace geos {
namespace geomgraph { // geos::geomgraph
namespace index { // geos::geomgraph::index

/**
 * A chain in a MonotoneChainEdge
 */
class GEOS_DLL MonotoneChain: public SweepLineEventOBJ {
private:
    MonotoneChainEdge* mce;
    size_t chainIndex;

    MonotoneChain(const MonotoneChain& other) = delete;
    MonotoneChain& operator=(const MonotoneChain& rhs) = delete;

public:

    MonotoneChain(MonotoneChainEdge* newMce, size_t newChainIndex):
        mce(newMce),
        chainIndex(newChainIndex)
    {}

    ~MonotoneChain() override {}

    void
    computeIntersections(MonotoneChain* mc, SegmentIntersector* si)
    {
        mce->computeIntersectsForChain(chainIndex, *(mc->mce), mc->chainIndex, *si);
    }
};


} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#endif

