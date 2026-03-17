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
 * Last port: geom/GeometryFactory.java rev. 1.48
 *
 **********************************************************************
 *
 * This is just a stub, there are a lot of candidates for inlining
 * but it's not worth checking that at the moment
 *
 **********************************************************************/

#ifndef GEOS_GEOM_GEOMETRYFACTORY_INL
#define GEOS_GEOM_GEOMETRYFACTORY_INL

#include <geos/geom/GeometryFactory.h>

namespace geos {
namespace geom { // geos::geom

INLINE int
GeometryFactory::getSRID() const
{
    return SRID;
}

INLINE const CoordinateSequenceFactory*
GeometryFactory::getCoordinateSequenceFactory() const
{
    return coordinateListFactory;
}

} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_GEOMETRYFACTORY_INL


