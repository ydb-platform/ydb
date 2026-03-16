/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/constants.h> // for int64
#include <geos/precision/CommonBits.h>

namespace geos {
namespace precision { // geos.precision

/*static public*/
int64
CommonBits::signExpBits(int64 num)
{
    return num >> 52;
}

/*static public*/
int
CommonBits::numCommonMostSigMantissaBits(int64 num1, int64 num2)
{
    int count = 0;
    for(int i = 52; i >= 0; i--) {
        if(getBit(num1, i) != getBit(num2, i)) {
            return count;
        }
        count++;
    }
    return 52;
}

/*static public*/
int64
CommonBits::zeroLowerBits(int64 bits, int nBits)
{
    if (nBits >= 64 || nBits < 0) return 0;
    const uint64_t bits_ = static_cast<uint64_t>(bits);
    const uint64_t invMask = (1ull << nBits) - 1;
    const uint64_t mask = ~ invMask;
    const uint64_t zeroed = bits_ & mask;
    return static_cast<int64>(zeroed);
}

/*static public*/
int
CommonBits::getBit(int64 bits, int i)
{
    int64 mask = (1ull << i);
    return (bits & mask) != 0 ? 1 : 0;
}

/*public*/
CommonBits::CommonBits()
{
    isFirst = true;
    commonMantissaBitsCount = 53;
    commonBits = 0;
}

/*public*/
void
CommonBits::add(double num)
{
    int64 numBits = (int64)num;
    if(isFirst) {
        commonBits = numBits;
        commonSignExp = signExpBits(commonBits);
        isFirst = false;
        return;
    }
    int64 numSignExp = signExpBits(numBits);
    if(numSignExp != commonSignExp) {
        commonBits = 0;
        return;
    }
    //    System.out.println(toString(commonBits));
    //    System.out.println(toString(numBits));
    commonMantissaBitsCount = numCommonMostSigMantissaBits(commonBits, numBits);
    commonBits = zeroLowerBits(commonBits, 64 - (12 + commonMantissaBitsCount));
    //    System.out.println(toString(commonBits));
}

/*public*/
double
CommonBits::getCommon()
{
    return (double)commonBits;
}

} // namespace geos.precision
} // namespace geos
