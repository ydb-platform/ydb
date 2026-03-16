/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/util.h>
#include <cmath>

namespace geos {
namespace util { // geos.util

/*
 * Symmetric Rounding Algorithm  - equivalent to C99 round()
 */
double
sym_round(double val)
{
    double n;
    double f = std::fabs(std::modf(val, &n));
    if(val >= 0) {
        if(f < 0.5) {
            return std::floor(val);
        }
        else if(f > 0.5) {
            return std::ceil(val);
        }
        else {
            return (n + 1.0);
        }
    }
    else {
        if(f < 0.5) {
            return std::ceil(val);
        }
        else if(f > 0.5) {
            return std::floor(val);
        }
        else {
            return (n - 1.0);
        }
    }
}

/*
 * Asymmetric Rounding Algorithm  - equivalent to Java Math.round()
 */
double
java_math_round(double val)
{
    double n;
    double f = std::fabs(std::modf(val, &n));

    if(val >= 0) {
        if(f < 0.5) {
            return std::floor(val);
        }
        else if(f > 0.5) {
            return std::ceil(val);
        }
        else {
            return (n + 1.0);
        }
    }
    else {
        if(f < 0.5) {
            return std::ceil(val);
        }
        else if(f > 0.5) {
            return std::floor(val);
        }
        else {
            return n;
        }
    }
} // java_math_round

/*
 * Implementation of rint()
 */
double
rint_vc(double val)
{
    double n;
    double f = std::fabs(std::modf(val, &n));
    if(val >= 0) {
        if(f < 0.5) {
            return std::floor(val);
        }
        else if(f > 0.5) {
            return std::ceil(val);
        }
        else {
            return(std::floor(n / 2) == n / 2) ? n : n + 1.0;
        }
    }
    else {
        if(f < 0.5) {
            return std::ceil(val);
        }
        else if(f > 0.5) {
            return std::floor(val);
        }
        else {
            return(std::floor(n / 2) == n / 2) ? n : n - 1.0;
        }
    }
}


} // namespace geos.util
} // namespace geos

