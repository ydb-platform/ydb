/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/PrecisionModel.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/geom/PrecisionModel.h>
#include <geos/geom/Coordinate.h>
#include <geos/util/IllegalArgumentException.h>
#include <geos/util/math.h>
#include <geos/util.h>

#include <sstream>
#include <string>
#include <cmath>
#include <iostream>

#ifndef GEOS_INLINE
# include <geos/geom/PrecisionModel.inl>
#endif

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;

namespace geos {
namespace geom { // geos::geom

const double PrecisionModel::maximumPreciseValue = 9007199254740992.0;

/*public*/
double
PrecisionModel::makePrecise(double val) const
{
#if GEOS_DEBUG
    cerr << "PrecisionModel[" << this << "]::makePrecise called" << endl;
#endif

    if(modelType == FLOATING_SINGLE) {
        float floatSingleVal = static_cast<float>(val);
        return static_cast<double>(floatSingleVal);
    }
    if(modelType == FIXED) {
        // Use whatever happens to be the default rounding method
        const double ret = util::round(val * scale) / scale;
        return ret;
    }
    // modelType == FLOATING - no rounding necessary
    return val;
}

/*public*/
void
PrecisionModel::makePrecise(Coordinate& coord) const
{
    // optimization for full precision
    if(modelType == FLOATING) {
        return;
    }

    coord.x = makePrecise(coord.x);
    coord.y = makePrecise(coord.y);
}


/*public*/
PrecisionModel::PrecisionModel()
    :
    modelType(FLOATING),
    scale(0.0)
{
#if GEOS_DEBUG
    cerr << "PrecisionModel[" << this << "] ctor()" << endl;
#endif
    //modelType=FLOATING;
    //scale=1.0;
}

/*public*/
PrecisionModel::PrecisionModel(Type nModelType)
    :
    modelType(nModelType),
    scale(1.0)
{
#if GEOS_DEBUG
    cerr << "PrecisionModel[" << this << "] ctor(Type)" << endl;
#endif
    //modelType=nModelType;
    //if (modelType==FIXED) setScale(1.0);
    //else setScale(666); // arbitrary number for invariant testing
}


/*public (deprecated) */
PrecisionModel::PrecisionModel(double newScale, double newOffsetX, double newOffsetY)
//throw(IllegalArgumentException *)
    :
    modelType(FIXED)
{
    ::geos::ignore_unused_variable_warning(newOffsetX);
    ::geos::ignore_unused_variable_warning(newOffsetY);

#if GEOS_DEBUG
    cerr << "PrecisionModel[" << this << "] ctor(scale,offsets)" << endl;
#endif

    //modelType = FIXED;
    setScale(newScale);
}

/*public*/
PrecisionModel::PrecisionModel(double newScale)
//throw (IllegalArgumentException *)
    :
    modelType(FIXED)
{
#if GEOS_DEBUG
    cerr << "PrecisionModel[" << this << "] ctor(scale)" << endl;
#endif
    setScale(newScale);
}

/*public*/
bool
PrecisionModel::isFloating() const
{
    return (modelType == FLOATING || modelType == FLOATING_SINGLE);
}

/*public*/
int
PrecisionModel::getMaximumSignificantDigits() const
{
    int maxSigDigits = 16;
    if(modelType == FLOATING) {
        maxSigDigits = 16;
    }
    else if(modelType == FLOATING_SINGLE) {
        maxSigDigits = 6;
    }
    else if(modelType == FIXED) {

        double dgtsd = std::log(getScale()) / std::log(double(10.0));
        const int dgts = static_cast<int>(
                             dgtsd > 0 ? std::ceil(dgtsd)
                             : std::floor(dgtsd)
                         );
        maxSigDigits = dgts;
    }
    return maxSigDigits;
}


/*private*/
void
PrecisionModel::setScale(double newScale)
// throw IllegalArgumentException
{
    if(newScale <= 0) {
        throw util::IllegalArgumentException("PrecisionModel scale cannot be 0");
    }
    scale = std::fabs(newScale);
}

/*public*/
double
PrecisionModel::getOffsetX() const
{
    return 0;
}

/*public*/
double
PrecisionModel::getOffsetY() const
{
    return 0;
}


string
PrecisionModel::toString() const
{
    ostringstream s;
    if(modelType == FLOATING) {
        s << "Floating";
    }
    else if(modelType == FLOATING_SINGLE) {
        s << "Floating-Single";
    }
    else if(modelType == FIXED) {
        s << "Fixed (Scale=" << getScale()
          << " OffsetX=" << getOffsetX()
          << " OffsetY=" << getOffsetY()
          << ")";
    }
    else {
        s << "UNKNOWN";
    }
    return s.str();
}

bool
operator==(const PrecisionModel& a, const PrecisionModel& b)
{
    return a.isFloating() == b.isFloating() &&
           a.getScale() == b.getScale();
}

/*public*/
int
PrecisionModel::compareTo(const PrecisionModel* other) const
{
    int sigDigits = getMaximumSignificantDigits();
    int otherSigDigits = other->getMaximumSignificantDigits();
    return sigDigits < otherSigDigits ? -1 : (sigDigits == otherSigDigits ? 0 : 1);
}

} // namespace geos::geom
} // namespace geos
