/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/shape/fractal/MortonCode.h>
#include <geos/geom/Coordinate.h>
#include <geos/util/IllegalArgumentException.h>

#include <cstdint>

namespace geos {
namespace shape {   // geos.shape
namespace fractal { // geos.shape.fractal


/**
* The number of points in the curve for the given level.
* The number of points is 2<sup>2 * level</sup>.
*
* @param level the level of the curve
* @return the number of points
*/
uint32_t
MortonCode::levelSize(uint32_t level)
{
    checkLevel(level);
    return (uint32_t) std::pow(2, 2 * level);
}

/**
* The maximum ordinate value for points
* in the curve for the given level.
* The maximum ordinate is 2<sup>level</sup> - 1.
*
* @param level the level of the curve
* @return the maximum ordinate value
*/
uint32_t
MortonCode::maxOrdinate(uint32_t level)
{
    checkLevel(level);
    return (uint32_t) std::pow(2, level) - 1;
}

/**
* The level of the finite Morton curve which contains at least
* the given number of points.
*
* @param numPoints the number of points required
* @return the level of the curve
*/
uint32_t
MortonCode::level(uint32_t numPoints)
{
    uint32_t pow2 = (uint32_t) ( (std::log(numPoints)/std::log(2)));
    uint32_t level = pow2 / 2;
    uint32_t sz = levelSize(level);
    if (sz < numPoints)
        level += 1;
    return level;
}

void
MortonCode::checkLevel(uint32_t level)
{
    if (level > MAX_LEVEL) {
        throw util::IllegalArgumentException("Level not in range");
    }
}

/**
* Computes the index of the point (x,y)
* in the Morton curve ordering.
*
* @param x the x ordinate of the point
* @param y the y ordinate of the point
* @return the index of the point along the Morton curve
*/
uint32_t
MortonCode::encode(int x, int y)
{
    return (interleave(y) << 1) + interleave(x);
}

uint32_t
MortonCode::interleave(uint32_t x)
{
    x &= 0x0000ffff;                  // x = ---- ---- ---- ---- fedc ba98 7654 3210
    x = (x ^ (x << 8)) & 0x00ff00ff; // x = ---- ---- fedc ba98 ---- ---- 7654 3210
    x = (x ^ (x << 4)) & 0x0f0f0f0f; // x = ---- fedc ---- ba98 ---- 7654 ---- 3210
    x = (x ^ (x << 2)) & 0x33333333; // x = --fe --dc --ba --98 --76 --54 --32 --10
    x = (x ^ (x << 1)) & 0x55555555; // x = -f-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0
    return x;
}

/**
* Computes the point on the Morton curve
* for a given index.
*
* @param index the index of the point on the curve
* @return the point on the curve
*/
geom::Coordinate
MortonCode::decode(uint32_t index)
{
    uint32_t x = deinterleave(index);
    uint32_t y = deinterleave(index >> 1);
    return geom::Coordinate(x, y);
}


uint32_t
MortonCode::deinterleave(uint32_t x)
{
    x = x & 0x55555555;
    x = (x | (x >> 1)) & 0x33333333;
    x = (x | (x >> 2)) & 0x0F0F0F0F;
    x = (x | (x >> 4)) & 0x00FF00FF;
    x = (x | (x >> 8)) & 0x0000FFFF;
    return x;
}


} // namespace geos.shape.fractal
} // namespace geos.shape
} // namespace geos
