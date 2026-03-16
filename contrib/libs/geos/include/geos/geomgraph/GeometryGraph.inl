/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geomgraph/GeometryGraph.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_GEOMETRYGRAPH_INL
#define GEOS_GEOMGRAPH_GEOMETRYGRAPH_INL

#include <geos/geomgraph/GeometryGraph.h>

namespace geos {
namespace geomgraph { // geos::geomgraph

INLINE void
GeometryGraph::getBoundaryNodes(std::vector<Node*>& bdyNodes)
{
    nodes->getBoundaryNodes(argIndex, bdyNodes);
}

INLINE const geom::Geometry*
GeometryGraph::getGeometry()
{
    return parentGeom;
}

INLINE
GeometryGraph::~GeometryGraph()
{
}

} // namespace geos::geomgraph
} // namespace geos

#endif // GEOS_GEOMGRAPH_GEOMETRYGRAPH_INL
