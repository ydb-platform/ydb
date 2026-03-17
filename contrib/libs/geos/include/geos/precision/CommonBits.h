/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_PRECISION_COMMONBITS_H
#define GEOS_PRECISION_COMMONBITS_H

#include <geos/export.h>
#include <geos/constants.h> // for int64

namespace geos {
namespace precision { // geos.precision

/** \brief
 * Determines the maximum number of common most-significant
 * bits in the mantissa of one or numbers.
 *
 * Can be used to compute the double-precision number which
 * is represented by the common bits.
 * If there are no common bits, the number computed is 0.0.
 *
 */
class GEOS_DLL CommonBits {

private:

    bool isFirst;

    int commonMantissaBitsCount;

    int64 commonBits;

    int64 commonSignExp;

public:

    /** \brief
     * Computes the bit pattern for the sign and exponent of a
     * double-precision number.
     *
     * @param num
     * @return the bit pattern for the sign and exponent
     */
    static int64 signExpBits(int64 num);

    /** \brief
     * This computes the number of common most-significant
     * bits in the mantissas of two double-precision numbers.
     *
     * It does not count the hidden bit, which is always 1.
     * It does not determine whether the numbers have the same
     * exponent - if they do not, the value computed by this
     * function is meaningless.
     * @param num1
     * @param num2
     * @return the number of common most-significant mantissa bits
     */
    static int numCommonMostSigMantissaBits(int64 num1, int64 num2);

    /** \brief
     * Zeroes the lower n bits of a bitstring.
     *
     * @param bits the bitstring to alter
     * @param nBits the number of bits to zero
     * @return the zeroed bitstring
     */
    static int64 zeroLowerBits(int64 bits, int nBits);

    /** \brief
     * Extracts the i'th bit of a bitstring.
     *
     * @param bits the bitstring to extract from
     * @param i the bit to extract
     * @return the value of the extracted bit
     */
    static int getBit(int64 bits, int i);

    CommonBits();

    void add(double num);

    double getCommon();

};

} // namespace geos.precision
} // namespace geos

#endif // GEOS_PRECISION_COMMONBITS_H
