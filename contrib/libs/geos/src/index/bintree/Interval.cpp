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

#include <geos/index/bintree/Interval.h>

namespace geos {
namespace index { // geos.index
namespace bintree { // geos.index.bintree

Interval::Interval()
{
    min = 0.0;
    max = 0.0;
}

Interval::Interval(double nmin, double nmax)
{
    init(nmin, nmax);
}

Interval::Interval(const Interval* interval)
{
    init(interval->min, interval->max);
}

void
Interval::init(double nmin, double nmax)
{
    min = nmin;
    max = nmax;
    if(min > max) {
        min = nmax;
        max = nmin;
    }
}

double
Interval::getMin() const
{
    return min;
}

double
Interval::getMax() const
{
    return max;
}

double
Interval::getWidth() const
{
    return max - min;
}

void
Interval::expandToInclude(Interval* interval)
{
    if(interval->max > max) {
        max = interval->max;
    }
    if(interval->min < min) {
        min = interval->min;
    }
}

bool
Interval::overlaps(const Interval* interval) const
{
    return overlaps(interval->min, interval->max);
}

bool
Interval::overlaps(double nmin, double nmax) const
{
    if(min > nmax || max < nmin) {
        return false;
    }
    return true;
}

bool
Interval::contains(const Interval* interval) const
{
    return contains(interval->min, interval->max);
}

bool
Interval::contains(double nmin, double nmax) const
{
    return (nmin >= min && nmax <= max);
}

bool
Interval::contains(double p) const
{
    return (p >= min && p <= max);
}

} // namespace geos.index.bintree
} // namespace geos.index
} // namespace geos
