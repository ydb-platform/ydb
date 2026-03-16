/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/GeometryCollection.java rev. 1.41
 *
 **********************************************************************/

#ifndef GEOS_GEOMETRYCOLLECTION_INL
#define GEOS_GEOMETRYCOLLECTION_INL

#include <geos/geom/GeometryCollection.h>

#include <vector>

namespace geos {
namespace geom { // geos::geom

INLINE GeometryCollection::const_iterator
GeometryCollection::begin() const
{
    return geometries.begin();
}

INLINE GeometryCollection::const_iterator
GeometryCollection::end() const
{
    return geometries.end();
}


} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOMETRYCOLLECTION_INL
