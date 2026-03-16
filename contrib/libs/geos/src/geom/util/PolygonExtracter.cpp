/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/


#include <geos/export.h>
#include <vector>

#include <geos/geom/GeometryComponentFilter.h>
#include <geos/geom/util/PolygonExtracter.h>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

void
PolygonExtracter::getPolygons(const Geometry& geom, std::vector<const Polygon*>& ret)
{
    PolygonExtracter pe(ret);
    geom.apply_ro(&pe);
}

PolygonExtracter::PolygonExtracter(std::vector<const Polygon*>& newComps)
    :
    comps(newComps)
{}

void
PolygonExtracter::filter_rw(Geometry* geom)
{
    if(const Polygon* p = dynamic_cast<const Polygon*>(geom)) {
        comps.push_back(p);
    }
}

void
PolygonExtracter::filter_ro(const Geometry* geom)
{
    if(const Polygon* p = dynamic_cast<const Polygon*>(geom)) {
        comps.push_back(p);
    }
}
}
}
}
