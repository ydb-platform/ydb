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

#include <geos/index/sweepline/SweepLineInterval.h>

namespace geos {
namespace index { // geos.index
namespace sweepline { // geos.index.sweepline

SweepLineInterval::SweepLineInterval(double newMin, double newMax, void* newItem)
{
    min = newMin < newMax ? newMin : newMax;
    max = newMax > newMin ? newMax : newMin;
    item = newItem;
}

double
SweepLineInterval::getMin()
{
    return min;
}

double
SweepLineInterval::getMax()
{
    return max;
}

void*
SweepLineInterval::getItem()
{
    return item;
}

} // namespace geos.index.sweepline
} // namespace geos.index
} // namespace geos

