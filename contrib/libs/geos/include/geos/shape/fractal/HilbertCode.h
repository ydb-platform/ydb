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


#pragma once

#include <geos/export.h>
#include <string>
#include <cstdint>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace shape {   // geos.shape
namespace fractal { // geos.shape.fractal

/**
 * Encodes points as the index along finite planar Hilbert curves.
 *
 * The planar Hilbert Curve is a continuous space-filling curve.
 * In the limit the Hilbert curve has infinitely many vertices and fills
 * the space of the unit square.
 * A sequence of finite approximations to the infinite Hilbert curve
 * is defined by the level number.
 * The finite Hilbert curve at level n H(n) contains 2^n+1 points.
 * Each finite Hilbert curve defines an ordering of the
 * points in the 2-dimensional range square containing the curve.
 * Curves fills the range square of side 2^level.
 * Curve points have ordinates in the range [0, 2^level - 1].
 * The index of a point along a Hilbert curve is called the Hilbert code.
 * The code for a given point is specific to the level chosen.
 *
 * This implementation represents codes using 32-bit integers.
 * This allows levels 0 to 16 to be handled.
 * The class supports encoding points in the range of a given level curve
 * and decoding the point for a given code value.
 *
 * The Hilbert order has the property that it tends to preserve locality.
 * This means that codes which are near in value will have spatially proximate
 * points.  The converse is not always true - the delta between
 * codes for nearby points is not always small.  But the average delta
 * is small enough that the Hilbert order is an effective way of linearizing space
 * to support range queries.
 *
 * @author Martin Davis
 *
 * @see MortonCode
 */
class GEOS_DLL HilbertCode {

public:

    /**
    * The maximum curve level that can be represented.
    */
    static constexpr int MAX_LEVEL = 16;

    static geom::Coordinate decode(uint32_t level, uint32_t i);

    static uint32_t encode(uint32_t level, uint32_t x, uint32_t y);

    /**
    * The number of points in the curve for the given level.
    * The number of points is 2^(2 * level).
    *
    * @param level the level of the curve
    * @return the number of points
    */
    static uint32_t levelSize(uint32_t level);

    /**
    * The maximum ordinate value for points
    * in the curve for the given level.
    * The maximum ordinate is 2^level - 1.
    *
    * @param level the level of the curve
    * @return the maximum ordinate value
    */
    static uint32_t maxOrdinate(uint32_t level);

    /**
    * The level of the finite Hilbert curve which contains at least
    * the given number of points.
    *
    * @param numPoints the number of points required
    * @return the level of the curve
    */
    static uint32_t level(uint32_t numPoints);


private:

    static uint32_t deinterleave(uint32_t x);

    static uint32_t interleave(uint32_t x);

    static uint32_t prefixScan(uint32_t x);

    static uint32_t descan(uint32_t x);

    static void checkLevel(int level);


};


} // namespace geos.shape.fractal
} // namespace geos.shape
} // namespace geos



