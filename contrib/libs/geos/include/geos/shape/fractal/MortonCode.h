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
 * Encodes points as the index along the planar Morton (Z-order) curve.
 *
 * The planar Morton (Z-order) curve is a continuous space-filling curve.
 * The Morton curve defines an ordering of the
 * points in the positive quadrant of the plane.
 * The index of a point along the Morton curve is called the Morton code.
 *
 * A sequence of subsets of the Morton curve can be defined by a level number.
 * Each level subset occupies a square range.
 * The curve at level n M(n) contains 2^(n + 1) points.
 * It fills the range square of side 2^level.
 * Curve points have ordinates in the range [0, 2^level - 1].
 * The code for a given point is identical at all levels.
 * The level simply determines the number of points in the curve subset
 * and the size of the range square.
 *
 * This implementation represents codes using 32-bit integers.
 * This allows levels 0 to 16 to be handled.
 * The class supports encoding points
 * and decoding the point for a given code value.
 *
 * The Morton order has the property that it tends to preserve locality.
 * This means that codes which are near in value will have spatially proximate
 * points.  The converse is not always true - the delta between
 * codes for nearby points is not always small.  But the average delta
 * is small enough that the Morton order is an effective way of linearizing space
 * to support range queries.
 *
 * @author Martin Davis
 *
 * @see HilbertCode
 */
class GEOS_DLL MortonCode {

public:

    /**
    * The maximum curve level that can be represented.
    */
    static constexpr int MAX_LEVEL = 16;

    /**
    * Computes the index of the point (x,y)
    * in the Morton curve ordering.
    *
    * @param x the x ordinate of the point
    * @param y the y ordinate of the point
    * @return the index of the point along the Morton curve
    */
    static uint32_t encode(int x, int y);

    /**
    * Computes the point on the Morton curve
    * for a given index.
    *
    * @param index the index of the point on the curve
    * @return the point on the curve
    */
    static geom::Coordinate decode(uint32_t index);

    /**
    * The number of points in the curve for the given level.
    * The number of points is 2<sup>2 * level</sup>.
    *
    * @param level the level of the curve
    * @return the number of points
    */
    static uint32_t levelSize(uint32_t level);

    /**
    * The maximum ordinate value for points
    * in the curve for the given level.
    * The maximum ordinate is 2<sup>level</sup> - 1.
    *
    * @param level the level of the curve
    * @return the maximum ordinate value
    */
    static uint32_t maxOrdinate(uint32_t level);

    /**
    * The level of the finite Morton curve which contains at least
    * the given number of points.
    *
    * @param numPoints the number of points required
    * @return the level of the curve
    */
    static uint32_t level(uint32_t numPoints);


private:

    static void checkLevel(uint32_t level) ;

    static  uint32_t interleave(uint32_t x);

    static  uint32_t deinterleave(uint32_t x);

};


} // namespace geos.shape.fractal
} // namespace geos.shape
} // namespace geos



