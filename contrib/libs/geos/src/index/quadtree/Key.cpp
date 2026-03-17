/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/quadtree/Key.java rev 1.8 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/index/quadtree/Key.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Coordinate.h>

#include <cmath>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace geos::geom;

namespace geos {
namespace index { // geos.index
namespace quadtree { // geos.index.quadtree

/* static public */
int
Key::computeQuadLevel(const Envelope& env)
{
    double dx = env.getWidth();
    double dy = env.getHeight();
    double dMax = dx > dy ? dx : dy;
    int level;
    frexp(dMax, &level);
#if GEOS_DEBUG
    std::cerr << "Maxdelta:" << dMax << " exponent:" << (level - 1) << std::endl;
#endif
    return level;
}

Key::Key(const Envelope& itemEnv)
    :
    pt(),
    level(0),
    env()
{
    computeKey(itemEnv);
}

const Coordinate&
Key::getPoint() const
{
    return pt;
}

int
Key::getLevel() const
{
    return level;
}

const Envelope&
Key::getEnvelope() const
{
    return env;
}

Coordinate*
Key::getCentre() const
{
    return new Coordinate(
               (env.getMinX() + env.getMaxX()) / 2,
               (env.getMinY() + env.getMaxY()) / 2
           );
}

/*public*/
void
Key::computeKey(const Envelope& itemEnv)
{
    level = computeQuadLevel(itemEnv);
    env.init(); // reset to null
    computeKey(level, itemEnv);
    // MD - would be nice to have a non-iterative form of this algorithm
    while(!env.contains(itemEnv)) {
        level += 1;
        computeKey(level, itemEnv);
    }
#if GEOS_DEBUG
    std::cerr << "Key::computeKey:" << std::endl;
    std::cerr << " itemEnv: " << itemEnv.toString() << std::endl;
    std::cerr << "  keyEnv: " << env.toString() << std::endl;
    std::cerr << "  keyLvl: " << level << std::endl;

#endif
}

void
Key::computeKey(int p_level, const Envelope& itemEnv)
{
    double quadSize = exp2(p_level);
    pt.x = std::floor(itemEnv.getMinX() / quadSize) * quadSize;
    pt.y = std::floor(itemEnv.getMinY() / quadSize) * quadSize;
    env.init(pt.x, pt.x + quadSize, pt.y, pt.y + quadSize);
}

} // namespace geos.index.quadtree
} // namespace geos.index
} // namespace geos
