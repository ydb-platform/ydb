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
#include <geos/geom/util/LinearComponentExtracter.h>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

LinearComponentExtracter::LinearComponentExtracter(std::vector<const LineString*>& newComps)
    :
    comps(newComps)
{}

void
LinearComponentExtracter::getLines(const Geometry& geom, std::vector<const LineString*>& ret)
{
    LinearComponentExtracter lce(ret);
    geom.apply_ro(&lce);
}

void
LinearComponentExtracter::filter_rw(Geometry* geom)
{
    if (geom->isEmpty()) return;
    if(const LineString* ls = dynamic_cast<const LineString*>(geom)) {
        comps.push_back(ls);
    }
}

void
LinearComponentExtracter::filter_ro(const Geometry* geom)
{
    if (geom->isEmpty()) return;
    if(const LineString* ls = dynamic_cast<const LineString*>(geom)) {
        comps.push_back(ls);
    }
}

}
}
}
