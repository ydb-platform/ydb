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
 **********************************************************************
 *
 * Last port: operation/overlay/MinimalEdgeRing.java rev. 1.13 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_MINIMALEDGERING_INL
#define GEOS_OP_OVERLAY_MINIMALEDGERING_INL

#include <geos/operation/overlay/MinimalEdgeRing.h>

#if GEOS_DEBUG
#include <iostream>
#endif

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay

INLINE void
MinimalEdgeRing::setEdgeRing(geomgraph::DirectedEdge* de, geomgraph::EdgeRing* er)
{
    de->setMinEdgeRing(er);
}

INLINE geomgraph::DirectedEdge*
MinimalEdgeRing::getNext(geomgraph::DirectedEdge* de)
{
    return de->getNextMin();
}

INLINE
MinimalEdgeRing::~MinimalEdgeRing()
{
#if GEOS_DEBUG
    std::cerr << "MinimalEdgeRing[" << this << "] dtor" << std::endl;
#endif
}

} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_OVERLAY_MINIMALEDGERING_INL

