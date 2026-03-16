/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
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

#include <geos/operation/overlay/MinimalEdgeRing.h>
#include <geos/geomgraph/EdgeRing.h>

#ifndef GEOS_INLINE
#include <geos/operation/overlay/MinimalEdgeRing.inl>
#endif

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
#include <iostream>
#endif

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay

MinimalEdgeRing::MinimalEdgeRing(geomgraph::DirectedEdge* start,
                                 const geom::GeometryFactory* p_geometryFactory)
    :
    geomgraph::EdgeRing(start, p_geometryFactory)
{
    computePoints(start);
    computeRing();
#if GEOS_DEBUG
    std::cerr << "MinimalEdgeRing[" << this << "] ctor" << std::endl;
#endif
}

} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

