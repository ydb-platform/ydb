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

#include <geos/index/bintree/Key.h>
#include <geos/index/bintree/Interval.h>

#include <cmath>

namespace geos {
namespace index { // geos.index
namespace bintree { // geos.index.bintree

int
Key::computeLevel(Interval* newInterval)
{
    double dx = newInterval->getWidth();
    int level;
    frexp(dx, &level);
    return level;
}

Key::Key(Interval* newInterval)
{
    interval = nullptr;
    pt = 0.0;
    level = 0;
    computeKey(newInterval);
}

Key::~Key()
{
    delete interval;
}

double
Key::getPoint()
{
    return pt;
}

int
Key::getLevel()
{
    return level;
}

Interval*
Key::getInterval()
{
    return interval;
}

/**
 * return a square envelope containing the argument envelope,
 * whose extent is a power of two and which is based at a power of 2
 */
void
Key::computeKey(Interval* itemInterval)
{
    level = computeLevel(itemInterval);
    delete interval;
    interval = new Interval();
    computeInterval(level, itemInterval);
    // MD - would be nice to have a non-iterative form of this algorithm
    while(!interval->contains(itemInterval)) {
        level += 1;
        computeInterval(level, itemInterval);
    }
}

void
Key::computeInterval(int p_level, Interval* itemInterval)
{
    double size = exp2(p_level);
    pt = std::floor(itemInterval->getMin() / size) * size;
    interval->init(pt, pt + size);
}

} // namespace geos.index.bintree
} // namespace geos.index
} // namespace geos
