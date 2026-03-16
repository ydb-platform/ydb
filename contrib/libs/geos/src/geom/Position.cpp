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
 **********************************************************************
 *
 * Last port: geom/Position.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/geom/Position.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

namespace geos {
namespace geom { // geos.geom

/**
 * Returns LEFT if the position is RIGHT, RIGHT if the position is LEFT, or the position
 * otherwise.
 */
int
Position::opposite(int position)
{
    if(position == LEFT) {
        return RIGHT;
    }
    if(position == RIGHT) {
        return LEFT;
    }
#if GEOS_DEBUG
    std::cerr << "Position::opposite: position is neither LEFT (" << LEFT << ") nor RIGHT (" << RIGHT << ") but " <<
              position << std::endl;
#endif
    return position;
}

} // namespace geos.geom
} // namespace geos
