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


#include <geos/geom/prep/PreparedLineString.h>
#include <geos/geom/prep/PreparedLineStringDistance.h>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep

double
PreparedLineStringDistance::distance(const geom::Geometry* g) const
{
    if ( prepLine.getGeometry().isEmpty() || g->isEmpty() )
    {
        return std::numeric_limits<double>::infinity();
    }

    // TODO: test if this shortcut be any useful
    //if ( prepLine.intersects(g) ) return 0.0;

    /* Not intersecting, compute distance from facets */
    operation::distance::IndexedFacetDistance *idf = prepLine.getIndexedFacetDistance();
    return idf->distance(g);
}


} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
