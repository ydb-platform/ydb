/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/ConvexHull.java r407 (JTS-1.12+)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_CONVEXHULL_INL
#define GEOS_ALGORITHM_CONVEXHULL_INL

#include <cassert>
#include <geos/algorithm/ConvexHull.h>
#include <geos/util/UniqueCoordinateArrayFilter.h>
#include <geos/geom/Geometry.h>

namespace geos {
namespace algorithm { // geos::algorithm

INLINE
ConvexHull::ConvexHull(const geom::Geometry* newGeometry)
    :
    geomFactory(newGeometry->getFactory())
{
    extractCoordinates(newGeometry);
}

INLINE
ConvexHull::~ConvexHull()
{
}

INLINE void
ConvexHull::extractCoordinates(const geom::Geometry* geom)
{
    util::UniqueCoordinateArrayFilter filter(inputPts);
    geom->apply_ro(&filter);
}

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_CONVEXHULL_INL
