/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>

#ifndef GEOS_INLINE
# include "geos/geom/CoordinateArraySequenceFactory.inl"
#endif

namespace geos {
namespace geom { // geos::geom

static CoordinateArraySequenceFactory defaultCoordinateSequenceFactory;

std::unique_ptr<CoordinateSequence>
CoordinateArraySequenceFactory::create() const
{
    return std::unique_ptr<CoordinateSequence>(
            new CoordinateArraySequence(
                    reinterpret_cast<std::vector<Coordinate>*>(0), 0));
}

const CoordinateSequenceFactory*
CoordinateArraySequenceFactory::instance()
{
    return &defaultCoordinateSequenceFactory;
}

} // namespace geos::geom
} // namespace geos

