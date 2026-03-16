/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: operation/GeometryGraphOperation.java rev. 1.18 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/GeometryGraphOperation.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/algorithm/BoundaryNodeRule.h>
#include <geos/geomgraph/GeometryGraph.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/PrecisionModel.h>

#include <cassert>

using namespace geos::algorithm;
using namespace geos::geomgraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation

//LineIntersector* GeometryGraphOperation::li=new LineIntersector();

GeometryGraphOperation::GeometryGraphOperation(const Geometry* g0,
        const Geometry* g1)
    :
    arg(2)
{
    const PrecisionModel* pm0 = g0->getPrecisionModel();
    assert(pm0);

    const PrecisionModel* pm1 = g1->getPrecisionModel();
    assert(pm1);

    // use the most precise model for the result
    if(pm0->compareTo(pm1) >= 0) {
        setComputationPrecision(pm0);
    }
    else {
        setComputationPrecision(pm1);
    }

    arg[0] = new GeometryGraph(0, g0,
                               algorithm::BoundaryNodeRule::getBoundaryOGCSFS());
    arg[1] = new GeometryGraph(1, g1,
                               algorithm::BoundaryNodeRule::getBoundaryOGCSFS());
}

GeometryGraphOperation::GeometryGraphOperation(const Geometry* g0,
        const Geometry* g1,
        const algorithm::BoundaryNodeRule& boundaryNodeRule)
    :
    arg(2)
{
    const PrecisionModel* pm0 = g0->getPrecisionModel();
    assert(pm0);

    const PrecisionModel* pm1 = g1->getPrecisionModel();
    assert(pm1);

    // use the most precise model for the result
    if(pm0->compareTo(pm1) >= 0) {
        setComputationPrecision(pm0);
    }
    else {
        setComputationPrecision(pm1);
    }

    arg[0] = new GeometryGraph(0, g0, boundaryNodeRule);
    arg[1] = new GeometryGraph(1, g1, boundaryNodeRule);
}


GeometryGraphOperation::GeometryGraphOperation(const Geometry* g0):
    arg(1)
{
    const PrecisionModel* pm0 = g0->getPrecisionModel();
    assert(pm0);

    setComputationPrecision(pm0);

    arg[0] = new GeometryGraph(0, g0);
}

const Geometry*
GeometryGraphOperation::getArgGeometry(unsigned int i) const
{
    assert(i < arg.size());
    return arg[i]->getGeometry();
}

/*protected*/
void
GeometryGraphOperation::setComputationPrecision(const PrecisionModel* pm)
{
    assert(pm);
    resultPrecisionModel = pm;
    li.setPrecisionModel(resultPrecisionModel);
}

GeometryGraphOperation::~GeometryGraphOperation()
{
    for(unsigned int i = 0; i < arg.size(); ++i) {
        delete arg[i];
    }
}

} // namespace geos.operation
} // namespace geos
