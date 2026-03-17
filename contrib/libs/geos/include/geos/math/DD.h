/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Crunchy Data
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

/**
 * Implements extended-precision floating-point numbers
 * which maintain 106 bits (approximately 30 decimal digits) of precision.
 * <p>
 * A DoubleDouble uses a representation containing two double-precision values.
 * A number x is represented as a pair of doubles, x.hi and x.lo,
 * such that the number represented by x is x.hi + x.lo, where
 * <pre>
 *    |x.lo| &lt;= 0.5*ulp(x.hi)
 * </pre>
 * and ulp(y) means "unit in the last place of y".
 * The basic arithmetic operations are implemented using
 * convenient properties of IEEE-754 floating-point arithmetic.
 * <p>
 * The range of values which can be represented is the same as in IEEE-754.
 * The precision of the representable numbers
 * is twice as great as IEEE-754 double precision.
 * <p>
 * The correctness of the arithmetic algorithms relies on operations
 * being performed with standard IEEE-754 double precision and rounding.
 * This is the Java standard arithmetic model, but for performance reasons
 * Java implementations are not
 * constrained to using this standard by default.
 * Some processors (notably the Intel Pentium architecture) perform
 * floating point operations in (non-IEEE-754-standard) extended-precision.
 * A JVM implementation may choose to use the non-standard extended-precision
 * as its default arithmetic mode.
 * To prevent this from happening, this code uses the
 * Java <tt>strictfp</tt> modifier,
 * which forces all operations to take place in the standard IEEE-754 rounding model.
 * <p>
 * The API provides both a set of value-oriented operations
 * and a set of mutating operations.
 * Value-oriented operations treat DoubleDouble values as
 * immutable; operations on them return new objects carrying the result
 * of the operation.  This provides a simple and safe semantics for
 * writing DoubleDouble expressions.  However, there is a performance
 * penalty for the object allocations required.
 * The mutable interface updates object values in-place.
 * It provides optimum memory performance, but requires
 * care to ensure that aliasing errors are not created
 * and constant values are not changed.
 * <p>
 * For example, the following code example constructs three DD instances:
 * two to hold the input values and one to hold the result of the addition.
 * <pre>
 *     DD a = new DD(2.0);
 *     DD b = new DD(3.0);
 *     DD c = a.add(b);
 * </pre>
 * In contrast, the following approach uses only one object:
 * <pre>
 *     DD a = new DD(2.0);
 *     a.selfAdd(3.0);
 * </pre>
 * <p>
 * This implementation uses algorithms originally designed variously by
 * Knuth, Kahan, Dekker, and Linnainmaa.
 * Douglas Priest developed the first C implementation of these techniques.
 * Other more recent C++ implementation are due to Keith M. Briggs and David Bailey et al.
 *
 * <h3>References</h3>
 * <ul>
 * <li>Priest, D., <i>Algorithms for Arbitrary Precision Floating Point Arithmetic</i>,
 * in P. Kornerup and D. Matula, Eds., Proc. 10th Symposium on Computer Arithmetic,
 * IEEE Computer Society Press, Los Alamitos, Calif., 1991.
 * <li>Yozo Hida, Xiaoye S. Li and David H. Bailey,
 * <i>Quad-Double Arithmetic: Algorithms, Implementation, and Application</i>,
 * manuscript, Oct 2000; Lawrence Berkeley National Laboratory Report BNL-46996.
 * <li>David Bailey, <i>High Precision Software Directory</i>;
 * <tt>http://crd.lbl.gov/~dhbailey/mpdist/index.html</tt>
 * </ul>
 *
 *
 * @author Martin Davis
 *
 */

#ifndef GEOS_MATH_DD_H
#define GEOS_MATH_DD_H

#include <cmath>

namespace geos {
namespace math { // geos.math

/**
 * \class DD
 *
 * \brief
 * Wrapper for DoubleDouble higher precision mathematics
 * operations.
 */
class GEOS_DLL DD {
    private:
        static constexpr double SPLIT = 134217729.0; // 2^27+1, for IEEE double
        double hi;
        double lo;

        int signum() const;
        DD rint() const;


    public:
        DD(double p_hi, double p_lo) : hi(p_hi), lo(p_lo) {};
        DD(double x) : hi(x), lo(0.0) {};
        DD() : hi(0.0), lo(0.0) {};

        bool operator==(const DD &rhs) const
        {
            return hi == rhs.hi && lo == rhs.lo;
        }

        bool operator!=(const DD &rhs) const
        {
            return hi != rhs.hi || lo != rhs.lo;
        }

        bool operator<(const DD &rhs) const
        {
            return (hi < rhs.hi) || (hi == rhs.hi && lo < rhs.lo);
        }

        bool operator<=(const DD &rhs) const
        {
            return (hi < rhs.hi) || (hi == rhs.hi && lo <= rhs.lo);
        }

        bool operator>(const DD &rhs) const
        {
            return (hi > rhs.hi) || (hi == rhs.hi && lo > rhs.lo);
        }

        bool operator>=(const DD &rhs) const
        {
            return (hi > rhs.hi) || (hi == rhs.hi && lo >= rhs.lo);
        }

        friend GEOS_DLL DD operator+ (const DD &lhs, const DD &rhs);
        friend GEOS_DLL DD operator+ (const DD &lhs, double rhs);
        friend GEOS_DLL DD operator- (const DD &lhs, const DD &rhs);
        friend GEOS_DLL DD operator- (const DD &lhs, double rhs);
        friend GEOS_DLL DD operator* (const DD &lhs, const DD &rhs);
        friend GEOS_DLL DD operator* (const DD &lhs, double rhs);
        friend GEOS_DLL DD operator/ (const DD &lhs, const DD &rhs);
        friend GEOS_DLL DD operator/ (const DD &lhs, double rhs);

        static DD determinant(const DD &x1, const DD &y1, const DD &x2, const DD &y2);
        static DD determinant(double x1, double y1, double x2, double y2);
        static DD abs(const DD &d);
        static DD pow(const DD &d, int exp);
        static DD trunc(const DD &d);

        bool isNaN() const;
        bool isNegative() const;
        bool isPositive() const;
        bool isZero() const;
        double doubleValue() const;
        double ToDouble() const { return doubleValue(); }
        int intValue() const;
        DD negate() const;
        DD reciprocal() const;
        DD floor() const;
        DD ceil() const;

        void selfAdd(const DD &d);
        void selfAdd(double p_hi, double p_lo);
        void selfAdd(double y);

        void selfSubtract(const DD &d);
        void selfSubtract(double p_hi, double p_lo);
        void selfSubtract(double y);

        void selfMultiply(double p_hi, double p_lo);
        void selfMultiply(const DD &d);
        void selfMultiply(double y);

        void selfDivide(double p_hi, double p_lo);
        void selfDivide(const DD &d);
        void selfDivide(double y);
};


} // namespace geos::math
} // namespace geos


#endif // GEOS_MATH_DD_H
