/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_UTIL_MATH_H
#define GEOS_UTIL_MATH_H

namespace geos {
namespace util {

/// Symmetric Rounding Algorithm
double sym_round(double val);

/// Asymmetric Rounding Algorithm
double java_math_round(double val);

/// Equivalent to Java Math.rint()
double rint_vc(double val);

/// Default rounding method for GEOS
///
/// @note Always use this rounding method, to easy easy switching
/// between different rounding method for the whole codebase.
inline double
round(double val)
{
    return java_math_round(val);
}

}
} // namespace geos::util

#endif // GEOS_UTIL_MATH_H
