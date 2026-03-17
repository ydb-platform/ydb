/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <cassert>

#include <geos/geom/GeometryComponentFilter.h>
#include <geos/geom/Geometry.h>
#include <geos/util.h>

namespace geos {
namespace geom { // geos::geom

void
GeometryComponentFilter::filter_rw(Geometry* geom)
{
    ::geos::ignore_unused_variable_warning(geom);
    assert(0);
}

void
GeometryComponentFilter::filter_ro(const Geometry* geom)
{
    ::geos::ignore_unused_variable_warning(geom);
    assert(0);
}


} // namespace geos::geom
} // namespace geos
