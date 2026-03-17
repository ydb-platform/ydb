/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/index/strtree/Interval.h>
//#include <geos/util.h>

#include <algorithm>
#include <typeinfo>
#include <cassert>

using namespace std;

namespace geos {
namespace index { // geos.index
namespace strtree { // geos.index.strtree

Interval::Interval(double newMin, double newMax)
{
    assert(newMin <= newMax);
    imin = newMin;
    imax = newMax;
}

double
Interval::getCentre()
{
    return (imin + imax) / 2;
}

Interval*
Interval::expandToInclude(const Interval* other)
{
    imax = max(imax, other->imax);
    imin = min(imin, other->imin);
    return this;
}

bool
Interval::intersects(const Interval* other) const
{
    return !(other->imin > imax || other->imax < imin);
}

bool
Interval::equals(const Interval* other) const
{
    return imin == other->imin && imax == other->imax;
}

} // namespace geos.index.strtree
} // namespace geos.index
} // namespace geos

