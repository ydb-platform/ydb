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

#include <geos/noding/BasicSegmentString.h>
#include <geos/geom/CoordinateSequence.h>

#ifndef GEOS_INLINE
# include <geos/noding/BasicSegmentString.inl>
#endif

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#include <iostream>
#include <sstream>

using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding

/* public virtual */
std::ostream&
BasicSegmentString::print(std::ostream& os) const
{
    os << "BasicSegmentString: " << std::endl;
    os << " LINESTRING" << *(pts) << ";" << std::endl;

    return os;
}


} // namespace geos.noding
} // namespace geos

