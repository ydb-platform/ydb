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
 * Last port: geom/MultiPolygon.java rev 1.34
 *
 **********************************************************************/

#ifndef GEOS_GEOM_MULTIPOLYGON_INL
#define GEOS_GEOM_MULTIPOLYGON_INL

#include <geos/geom/MultiPolygon.h>
#include <geos/geom/GeometryCollection.h>

namespace geos {
namespace geom { // geos::geom

INLINE
MultiPolygon::MultiPolygon(const MultiPolygon& mp)
    :
    GeometryCollection(mp)
{
}

INLINE std::unique_ptr<Geometry>
MultiPolygon::clone() const
{
    return std::unique_ptr<Geometry>(new MultiPolygon(*this));
}

} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_MULTIPOLYGON_INL


