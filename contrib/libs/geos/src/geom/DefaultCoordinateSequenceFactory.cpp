/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/geom/DefaultCoordinateSequenceFactory.h>

namespace geos {
namespace geom { // geos::geom

static DefaultCoordinateSequenceFactory defaultCoordinateSequenceFactory;

const CoordinateSequenceFactory*
DefaultCoordinateSequenceFactory::instance()
{
    return &defaultCoordinateSequenceFactory;
}

} // namespace geos::geom
} // namespace geos

