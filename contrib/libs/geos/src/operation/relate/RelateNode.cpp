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
 * Last port: operation/relate/RelateNode.java rev. 1.11 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/relate/RelateNode.h>
#include <geos/operation/relate/EdgeEndBundleStar.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geomgraph/Label.h>
#include <geos/geomgraph/Node.h>
#include <geos/util.h>

#include <cassert>

using namespace geos::geomgraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace relate { // geos.operation.relate

RelateNode::RelateNode(const Coordinate& p_coord, EdgeEndStar* p_edges):
    Node(p_coord, p_edges)
{}

/**
 * Update the IM with the contribution for this component.
 * A component only contributes if it has a labelling for both parent geometries
 */
void
RelateNode::computeIM(IntersectionMatrix& im)
{
    im.setAtLeastIfValid(label.getLocation(0), label.getLocation(1), 0);
}

void
RelateNode::updateIMFromEdges(IntersectionMatrix& im)
{
    EdgeEndBundleStar* eebs = detail::down_cast<EdgeEndBundleStar*>(edges);

    eebs->updateIM(im);
}

} // namespace geos.operation.relate
} // namespace geos.operation
} // namespace geos
