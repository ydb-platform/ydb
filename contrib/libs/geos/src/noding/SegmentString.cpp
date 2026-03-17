/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011      Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/SegmentString.java r430 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/noding/SegmentString.h>
//#include <geos/noding/SegmentNodeList.h>
//#include <geos/algorithm/LineIntersector.h>
//#include <geos/geom/Coordinate.h>
//#include <geos/geom/CoordinateSequence.h>
//#include <geos/util/IllegalArgumentException.h>
//#include <geos/noding/Octant.h>
//#include <geos/profiler.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#include <iostream>
#include <sstream>

using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding

std::ostream&
operator<< (std::ostream& os, const SegmentString& ss)
{
    return ss.print(os);
}

std::ostream&
SegmentString::print(std::ostream& os) const
{
    os << "SegmentString" << std::endl;
    return os;
}

} // namespace geos.noding
} // namespace geos

