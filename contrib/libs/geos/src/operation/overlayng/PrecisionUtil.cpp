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

#include <geos/operation/overlayng/PrecisionUtil.h>

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/PrecisionModel.h>

#include <sstream>

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;


/*public static*/
PrecisionModel
PrecisionUtil::robustPM(const Geometry* a, const Geometry* b)
{
    double scale = PrecisionUtil::robustScale(a, b);
    PrecisionModel pm(scale);
    return pm;
}

/*public static*/
PrecisionModel
PrecisionUtil::robustPM(const Geometry* a)
{
    double scale = PrecisionUtil::robustScale(a);
    PrecisionModel pm(scale);
    return pm;
}

/*public static*/
double
PrecisionUtil::robustScale(const Geometry* a, const Geometry* b)
{
    double inhSc = inherentScale(a, b);
    double sfScl = safeScale(a, b);
    return robustScale(inhSc, sfScl);
}

/*public static*/
double
PrecisionUtil::robustScale(const Geometry* a)
{
    double inhSc = inherentScale(a);
    double sfScl = safeScale(a);
    return robustScale(inhSc, sfScl);
}

/*private static*/
double
PrecisionUtil::robustScale(double inhSc, double sfScl)
{
    /**
    * Use safe scale if lower,
    * since it is important to preserve some precision for robustness
    */
    if (inhSc <= sfScl) {
        return inhSc;
    }
    return sfScl;
}

/*public static*/
double
PrecisionUtil::safeScale(double value)
{
    return precisionScale(value, MAX_ROBUST_DP_DIGITS);
}

/*public static*/
double
PrecisionUtil::safeScale(const Geometry* geom)
{
    return safeScale(maxBoundMagnitude(geom->getEnvelopeInternal()));
}

/*public static*/
double
PrecisionUtil::safeScale(const Geometry* a, const Geometry* b)
{
    double maxBnd = maxBoundMagnitude(a->getEnvelopeInternal());
    if (b != nullptr) {
        double maxBndB = maxBoundMagnitude(b->getEnvelopeInternal());
        maxBnd = std::max(maxBnd, maxBndB);
    }
    double scale = PrecisionUtil::safeScale(maxBnd);
    return scale;
}

/*private static*/
double
PrecisionUtil::maxBoundMagnitude(const Envelope* env)
{
    return std::max(
        std::max(
            std::abs(env->getMaxX()),
            std::abs(env->getMaxY())),
        std::max(
            std::abs(env->getMinX()),
            std::abs(env->getMinY())));
}

/*private static*/
double
PrecisionUtil::precisionScale(double value, int precisionDigits)
{
    // the smallest power of 10 greater than the value
    int magnitude = (int) (std::log(value) / std::log(10) + 1.0);
    int precDigits = precisionDigits - magnitude;

    double scaleFactor = std::pow(10.0, precDigits);
    return scaleFactor;
}

/*public static*/
double
PrecisionUtil::inherentScale(double value)
{
    int numDec = numberOfDecimals(value);
    double scaleFactor = std::pow(10.0, numDec);
    return scaleFactor;
}

/*public static*/
double
PrecisionUtil::inherentScale(const Geometry* geom)
{
    InherentScaleFilter scaleFilter;;
    geom->apply_ro(&scaleFilter);
    return scaleFilter.getScale();
}

/*public static*/
double
PrecisionUtil::inherentScale(const Geometry* a, const Geometry* b)
{
    double scale = PrecisionUtil::inherentScale(a);
    if (b != nullptr) {
        double scaleB = PrecisionUtil::inherentScale(b);
        scale = std::max(scale, scaleB);
    }
    return scale;
}

/*public static*/
// int
// PrecisionUtil::numberOfDecimals(double value)
// {
//     /**
//     * Ensure that scientific notation is NOT used
//     * (it would skew the number of fraction digits)
//     */
//     std::stringstream ss;
//     ss.precision(10);
//     ss << value;
//     std::string s = ss.str();
//     std::size_t len = s.length();
//     std::size_t dotZero = s.rfind(".0");
//     if (dotZero == len-2)
//         return 0;
//     std::size_t decIndex = s.find_last_of(".");
//     if (decIndex == std::string::npos)
//         return 0;
//     if (decIndex == 0)
//         return 0;
//     return len - decIndex - 1;
// }


/*public static*/
int
PrecisionUtil::numberOfDecimals(double value)
{
    int digits = 0;
    double threshold = 0.00005;
    while (std::fabs(value - std::round(value)) > threshold) {
        digits++;
        value *= 10.0;
        if (digits >= 17)
            return digits;
    }
    return digits;
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
