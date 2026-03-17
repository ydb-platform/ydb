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

#include <geos/index/chain/MonotoneChainOverlapAction.h>
#include <geos/index/chain/MonotoneChain.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/LineSegment.h>
#include <geos/profiler.h>

//#include <stdio.h>

namespace geos {
namespace index { // geos.index
namespace chain { // geos.index.chain

void
MonotoneChainOverlapAction::overlap(MonotoneChain& mc1, size_t start1,
                                    MonotoneChain& mc2, size_t start2)
{
    mc1.getLineSegment(start1, overlapSeg1);
    mc2.getLineSegment(start2, overlapSeg2);
    overlap(overlapSeg1, overlapSeg2);
}

} // namespace geos.index.chain
} // namespace geos.index
} // namespace geos
