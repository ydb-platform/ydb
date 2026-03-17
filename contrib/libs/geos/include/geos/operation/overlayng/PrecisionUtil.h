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

#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/Coordinate.h>

#include <vector>
#include <map>


// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class Envelope;
class PrecisionModel;
}
namespace operation {
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/**
 * Unions a collection of geometries in an
 * efficient way, using {@link OverlayNG}
 * to ensure robust computation.
 * @author Martin Davis
 */
class GEOS_DLL PrecisionUtil {

private:

    static double robustScale(double inherentScale, double safeScale);

    /**
    * Determines the maximum magnitude (absolute value) of the bounds of an
    * of an envelope.
    * This is equal to the largest ordinate value
    * which must be accommodated by a scale factor.
    *
    */
    static double maxBoundMagnitude(const Envelope* env);

    /**
    * Computes the scale factor which will
    * produce a given number of digits of precision (significant digits)
    * when used to round the given number.
    *
    * For example: to provide 5 decimal digits of precision
    * for the number 123.456 the precision scale factor is 100;
    * for 3 digits of precision the scale factor is 1;
    * for 2 digits of precision the scale factor is 0.1.
    *
    * Rounding to the scale factor can be performed with {@link PrecisionModel#round}
    *
    * @see PrecisionModel.round
    */
    static double precisionScale(double value, int precisionDigits);



public:

    static constexpr int MAX_ROBUST_DP_DIGITS = 14;

    PrecisionUtil() {};

    /**
    * Determines a precision model to
    * use for robust overlay operations.
    * The precision scale factor is chosen to maximize
    * output precision while avoiding round-off issues.
    *
    * NOTE: this is a heuristic determination, so is not guaranteed to
    * eliminate precision issues.
    *
    * WARNING: this is quite slow.
    */
    static PrecisionModel robustPM(const Geometry* a, const Geometry* b);

    /**
    * Determines a precision model to
    * use for robust overlay operations for one geometry.
    * The precision scale factor is chosen to maximize
    * output precision while avoiding round-off issues.
    *
    * NOTE: this is a heuristic determination, so is not guaranteed to
    * eliminate precision issues.
    *
    * WARNING: this is quite slow.
    */
    static PrecisionModel robustPM(const Geometry* a);

    /**
    * Determines a scale factor which maximizes
    * the digits of precision and is
    * safe to use for overlay operations.
    * The robust scale is the minimum of the
    * inherent scale and the safe scale factors.
    */
    static double robustScale(const Geometry* a, const Geometry* b);

    /**
    * Determines a scale factor which maximizes
    * the digits of precision and is
    * safe to use for overlay operations.
    * The robust scale is the minimum of the
    * inherent scale and the safe scale factors.
    */
    static double robustScale(const Geometry* a);

    /**
    * Computes a safe scale factor for a numeric value.
    * A safe scale factor ensures that rounded
    * number has no more than MAX_PRECISION_DIGITS
    * digits of precision.
    */
    static double safeScale(double value);

    /**
    * Computes a safe scale factor for a geometry.
    * A safe scale factor ensures that the rounded
    * ordinates have no more than MAX_PRECISION_DIGITS
    * digits of precision.
    */
    static double safeScale(const Geometry* geom);

    /**
    * Computes a safe scale factor for two geometries.
    * A safe scale factor ensures that the rounded
    * ordinates have no more than MAX_PRECISION_DIGITS
    * digits of precision.
    */
    static double safeScale(const Geometry* a, const Geometry* b);

    /**
    * Computes the inherent scale of a number.
    * The inherent scale is the scale factor for rounding
    * which preserves all digits of precision
    * (significant digits)
    * present in the numeric value.
    * In other words, it is the scale factor which does not
    * change the numeric value when rounded:
    *
    *   num = round( num, inherentScale(num) )
    */
    static double inherentScale(double value);

    /**
    * Computes the inherent scale of a geometry.
    * The inherent scale is the scale factor for rounding
    * which preserves <b>all</b> digits of precision
    * (significant digits)
    * present in the geometry ordinates.
    *
    * This is the maximum inherent scale
    * of all ordinate values in the geometry.
    */
    static double inherentScale(const Geometry* geom);

    /**
    * Computes the inherent scale of two geometries.
    * The inherent scale is the scale factor for rounding
    * which preserves <b>all</b> digits of precision
    * (significant digits)
    * present in the geometry ordinates.
    *
    * This is the maximum inherent scale
    * of all ordinate values in the geometries.
    */
    static double inherentScale(const Geometry* a, const Geometry* b);

    /**
    * Determines the
    * number of decimal places represented in a double-precision
    * number (as determined by Java).
    * This uses the Java double-precision print routine
    * to determine the number of decimal places,
    * This is likely not optimal for performance,
    * but should be accurate and portable.
    */
    static int numberOfDecimals(double value);

    /**
    * Applies the inherent scale calculation
    * to every ordinate in a geometry.
    */
    class GEOS_DLL InherentScaleFilter: public CoordinateFilter {

        private:

            double scale;

            void updateScaleMax(double value) {
                double scaleVal = PrecisionUtil::inherentScale(value);
                if (scaleVal > scale) {
                    scale = scaleVal;
                }
            }

        public:

            InherentScaleFilter()
                : scale(0.0)
                {}

            void filter_ro(const geom::Coordinate* coord) override
            {
                updateScaleMax(coord->x);
                updateScaleMax(coord->y);
            }

            double getScale() const {
                return scale;
            }
    };


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

