/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/prep/PreparedGeometryFactory.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/


#include <geos/geom/Point.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>
#include <geos/geom/prep/PreparedGeometry.h>
#include <geos/geom/prep/BasicPreparedGeometry.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/geom/prep/PreparedLineString.h>
#include <geos/geom/prep/PreparedPoint.h>
#include <geos/util/IllegalArgumentException.h>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep

std::unique_ptr<PreparedGeometry>
PreparedGeometryFactory::create(const geom::Geometry* g) const
{
    using geos::geom::GeometryTypeId;

    if(nullptr == g) {
        throw util::IllegalArgumentException("PreparedGeometry constructed with null Geometry object");
    }

    std::unique_ptr<PreparedGeometry> pg;

    switch(g->getGeometryTypeId()) {
    case GEOS_MULTIPOINT:
    case GEOS_POINT:
        pg.reset(new PreparedPoint(g));
        break;

    case GEOS_LINEARRING:
    case GEOS_LINESTRING:
    case GEOS_MULTILINESTRING:
        pg.reset(new PreparedLineString(g));
        break;

    case GEOS_POLYGON:
    case GEOS_MULTIPOLYGON:
        pg.reset(new PreparedPolygon(g));
        break;

    default:
        pg.reset(new BasicPreparedGeometry(g));
    }
    return pg;
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
