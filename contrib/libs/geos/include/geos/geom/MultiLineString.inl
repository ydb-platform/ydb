/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/MultiLineString.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_MULTILINESTRING_INL
#define GEOS_GEOM_MULTILINESTRING_INL

#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPoint.h>

//#include <cassert>

namespace geos {
namespace geom { // geos::geom

INLINE
MultiLineString::MultiLineString(const MultiLineString& mp)
    :
    GeometryCollection(mp)
{
}

INLINE std::unique_ptr<Geometry>
MultiLineString::clone() const
{
    return std::unique_ptr<Geometry>(new MultiLineString(*this));
}

} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_MULTILINESTRING_INL


