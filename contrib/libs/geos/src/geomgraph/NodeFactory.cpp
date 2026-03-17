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
 * Last port: geomgraph/NodeFactory.java rev. 1.3 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/geomgraph/NodeFactory.h>
#include <geos/geomgraph/Node.h>

using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

Node*
NodeFactory::createNode(const Coordinate& coord) const
{
    return new Node(coord, nullptr);
}

const NodeFactory&
NodeFactory::instance()
{
    static const NodeFactory nf;
    return nf;
}


} // namespace geos.geomgraph
} // namespace geos
