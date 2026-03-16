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
 * Last port: geom/PrecisionModel.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PRECISIONMODEL_INL
#define GEOS_GEOM_PRECISIONMODEL_INL

#include <geos/geom/PrecisionModel.h>

#include <cassert>

namespace geos {
namespace geom { // geos::geom

/*public*/
INLINE void
PrecisionModel::makePrecise(Coordinate* coord) const
{
    assert(coord);
    return makePrecise(*coord);
}

/*public*/
INLINE PrecisionModel::Type
PrecisionModel::getType() const
{
    return modelType;
}

/*public*/
INLINE double
PrecisionModel::getScale() const
{
    assert(!(scale < 0));
    return scale;
}


} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PRECISIONMODEL_INL

