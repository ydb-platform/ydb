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
 * Last port: operation/relate/RelateNodeFactory.java rev. 1.11 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/relate/RelateNodeFactory.h>
#include <geos/operation/relate/RelateNode.h>
#include <geos/operation/relate/EdgeEndBundleStar.h>

using namespace geos::geom;
using namespace geos::geomgraph;

namespace geos {
namespace operation { // geos.operation
namespace relate { // geos.operation.relate

Node*
RelateNodeFactory::createNode(const Coordinate& coord) const
{
    return new RelateNode(coord, new EdgeEndBundleStar());
}

const NodeFactory&
RelateNodeFactory::instance()
{
    static const RelateNodeFactory rnf;
    return rnf;
}


} // namespace geos.operation.relate
} // namespace geos.operation
} // namespace geos

