/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/BasicSegmentString.java rev. 1.1 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_NODING_BASICSEGMENTSTRING_INL
#define GEOS_NODING_BASICSEGMENTSTRING_INL

#include <cstddef>

#include <geos/noding/BasicSegmentString.h>
#include <geos/noding/Octant.h>

namespace geos {
namespace noding {

/*public*/
INLINE int
BasicSegmentString::getSegmentOctant(size_t index) const
{
    if(index >= size() - 1) {
        return -1;
    }
    return Octant::octant(getCoordinate(index), getCoordinate(index + 1));
}

/* virtual public */
INLINE const geom::Coordinate&
BasicSegmentString::getCoordinate(size_t i) const
{
    return pts->getAt(i);
}

/* virtual public */
INLINE geom::CoordinateSequence*
BasicSegmentString::getCoordinates() const
{
    return pts;
}

/* virtual public */
INLINE bool
BasicSegmentString::isClosed() const
{
    return pts->getAt(0) == pts->getAt(size() - 1);
}

}
}

#endif
