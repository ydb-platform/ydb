/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/overlayng/UnaryUnionNG.h>

#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/operation/overlayng/OverlayUtil.h>
#include <geos/operation/overlayng/PrecisionUtil.h>
#include <geos/operation/union/UnionStrategy.h>
#include <geos/operation/union/UnaryUnionOp.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/PrecisionModel.h>



namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/*public static*/
std::unique_ptr<Geometry>
UnaryUnionNG::Union(const Geometry* geom, const PrecisionModel& pm)
{
    NGUnionStrategy ngUnionStrat(pm);
    geounion::UnaryUnionOp op(*geom);
    op.setUnionFunction(&ngUnionStrat);
    return op.Union();
}

/*public static*/
std::unique_ptr<Geometry>
UnaryUnionNG::Union(const Geometry*  geom)
{
    PrecisionModel pm = PrecisionUtil::robustPM(geom);
    return UnaryUnionNG::Union(geom, pm);
}


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
