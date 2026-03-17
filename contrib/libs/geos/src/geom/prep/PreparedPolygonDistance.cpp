/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: ORIGINAL WORK
 *
 **********************************************************************/

#include <geos/geom/prep/PreparedPolygonDistance.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/geom/Geometry.h>

// std
#include <cstddef>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep

double
PreparedPolygonDistance::distance(const geom::Geometry* g) const
{
    if ( prepPoly.getGeometry().isEmpty() || g->isEmpty() )
    {
        return std::numeric_limits<double>::infinity();
    }

    if ( prepPoly.intersects(g) ) return 0.0;

    /* Not intersecting, compute distance from facets */
    operation::distance::IndexedFacetDistance *idf = prepPoly.getIndexedFacetDistance();
    return idf->distance(g);
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
