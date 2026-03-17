/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_POINTINRING_H
#define GEOS_ALGORITHM_POINTINRING_H

#include <geos/export.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace algorithm { // geos::algorithm

class GEOS_DLL PointInRing {
public:
    virtual
    ~PointInRing() {}
    virtual bool isInside(const geom::Coordinate& pt) = 0;
};

} // namespace geos::algorithm
} // namespace geos


#endif

