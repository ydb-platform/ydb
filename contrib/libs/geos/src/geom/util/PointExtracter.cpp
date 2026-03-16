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
#include <geos/geom/util/PointExtracter.h>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util


void
PointExtracter::getPoints(const Geometry& geom, Point::ConstVect& ret)
{
    PointExtracter pe(ret);
    geom.apply_ro(&pe);
}

/**
 * Constructs a PointExtracterFilter with a list in which
 * to store Points found.
 */
PointExtracter::PointExtracter(Point::ConstVect& newComps)
    :
    comps(newComps)
{}

void
PointExtracter::filter_rw(Geometry* geom)
{
    if(const Point* p = dynamic_cast<const Point*>(geom)) {
        comps.push_back(p);
    }
}

void
PointExtracter::filter_ro(const Geometry* geom)
{
    if(const Point* p = dynamic_cast<const Point*>(geom)) {
        comps.push_back(p);
    }
}
}
}
}
