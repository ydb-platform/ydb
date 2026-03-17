/**********************************************************************
 *
 * constants.h
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2018 Vicky Vergara
 * Copyright (C) 2009 Mateusz Loskot
 * Copyright (C) 2005-2009 Refractions Research Inc.
 * Copyright (C) 2001-2009 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 *********************************************************************/

#ifndef INCLUDE_GEOS_CONSTANTS_H_
#define INCLUDE_GEOS_CONSTANTS_H_

#ifdef _MSC_VER
#ifndef NOMINMAX
#define NOMINMAX 1
typedef __int64 int64;
#endif
#endif

#include <cmath>
#include <limits>
#include <cinttypes>


typedef int64_t int64;

namespace geos {

constexpr double MATH_PI = 3.14159265358979323846;



// Some handy constants
constexpr double DoubleNotANumber = std::numeric_limits<double>::quiet_NaN();
constexpr double DoubleMax = (std::numeric_limits<double>::max)();
constexpr double DoubleInfinity = (std::numeric_limits<double>::infinity)();
constexpr double DoubleNegInfinity = (-(std::numeric_limits<double>::infinity)());

}  // namespace geos


#endif  // INCLUDE_GEOS_CONSTANTS_H_
