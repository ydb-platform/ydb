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
 * Last port: operation/relate/RelateOp.java rev. 1.19 (JTS-1.10)
 *
 **********************************************************************/


#include <geos/operation/relate/RelateComputer.h>
#include <geos/operation/relate/RelateOp.h>

// Forward declarations
namespace geos {
namespace geom {
class IntersectionMatrix;
class Geometry;
}
}

using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace relate { // geos.operation.relate

std::unique_ptr<IntersectionMatrix>
RelateOp::relate(const Geometry* a, const Geometry* b)
{
    RelateOp relOp(a, b);
    return relOp.getIntersectionMatrix();
}

std::unique_ptr<IntersectionMatrix>
RelateOp::relate(const Geometry* a, const Geometry* b,
                 const algorithm::BoundaryNodeRule& boundaryNodeRule)
{
    RelateOp relOp(a, b, boundaryNodeRule);
    return relOp.getIntersectionMatrix();
}

RelateOp::RelateOp(const Geometry* g0, const Geometry* g1):
    GeometryGraphOperation(g0, g1),
    relateComp(&arg)
{
}

RelateOp::RelateOp(const Geometry* g0, const Geometry* g1,
                   const algorithm::BoundaryNodeRule& boundaryNodeRule)
    :
    GeometryGraphOperation(g0, g1, boundaryNodeRule),
    relateComp(&arg)
{
}

std::unique_ptr<IntersectionMatrix>
RelateOp::getIntersectionMatrix()
{
    return relateComp.computeIM();
}

} // namespace geos.operation.relate
} // namespace geos.operation
} // namespace geos

