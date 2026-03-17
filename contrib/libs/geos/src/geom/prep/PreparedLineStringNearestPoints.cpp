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
#include <geos/geom/prep/PreparedLineStringNearestPoints.h>
#include <geos/operation/distance/IndexedFacetDistance.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/GeometryFactory.h>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep

std::unique_ptr<geom::CoordinateSequence>
PreparedLineStringNearestPoints::nearestPoints(const geom::Geometry* g) const
{
    const GeometryFactory *gf = prepLine.getGeometry().getFactory();
    const CoordinateSequenceFactory *cf = gf->getCoordinateSequenceFactory();
    operation::distance::IndexedFacetDistance *idf = prepLine.getIndexedFacetDistance();
    return cf->create(idf->nearestPoints(g));
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
