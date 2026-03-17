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
 * Last port: operation/overlay/OverlayNodeFactory.java rev. 1.11 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/overlay/OverlayNodeFactory.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/DirectedEdgeStar.h>

using namespace geos::geomgraph;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay

Node*
OverlayNodeFactory::createNode(const geom::Coordinate& coord) const
{
    return new Node(coord, new DirectedEdgeStar());
}

const NodeFactory&
OverlayNodeFactory::instance()
{
    static OverlayNodeFactory onf;
    return onf;
}

} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

