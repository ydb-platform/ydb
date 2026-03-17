/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/chain/MonotoneChainSelectAction.java rev. 1.6 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/index/chain/MonotoneChainSelectAction.h>
#include <geos/index/chain/MonotoneChain.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/LineSegment.h>


namespace geos {
namespace index { // geos.index
namespace chain { // geos.index.chain

void
MonotoneChainSelectAction::select(MonotoneChain& mc, size_t start)
{
    mc.getLineSegment(start, selectedSegment);

    select(selectedSegment);
}

} // namespace geos.index.chain
} // namespace geos.index
} // namespace geos
