/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/geom/Location.h>
#include <geos/util/IllegalArgumentException.h>

#include <sstream>

using namespace std;

namespace geos {
namespace geom { // geos::geom

std::ostream&
operator<<(std::ostream& os, const Location& loc)
{
    switch(loc) {
        case Location::EXTERIOR:
            os << 'e';
            break;
        case Location::BOUNDARY:
            os << 'b';
            break;
        case Location::INTERIOR:
            os << 'i';
            break;
        case Location::NONE:
            os << '-';
            break;
    }
    return os;
}

} // namespace geos::geom
} // namespace geos
