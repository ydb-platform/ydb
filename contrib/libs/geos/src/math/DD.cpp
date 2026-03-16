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

#include <cmath>

#include <geos/profiler.h>
#include <geos/math/DD.h>

namespace geos {
namespace math { // geos.util



/* public */
bool DD::isNaN() const
{
    return std::isnan(hi);
}
/* public */
bool DD::isNegative() const
{
    return hi < 0.0 || (hi == 0.0 && lo < 0.0);
}
/* public */
bool DD::isPositive() const
{
    return hi > 0.0 || (hi == 0.0 && lo > 0.0);
}
/* public */
bool DD::isZero() const
{
    return hi == 0.0 && lo == 0.0;
}

/* public */
double DD::doubleValue() const
{
    return hi + lo;
}

/* public */
int DD::intValue() const
{
    return (int) hi;
}

/* public */
void DD::selfAdd(const DD &y)
{
    return selfAdd(y.hi, y.lo);
}

/* public */
void DD::selfAdd(double yhi, double ylo)
{
    double H, h, T, t, S, s, e, f;
    S = hi + yhi;
    T = lo + ylo;
    e = S - hi;
    f = T - lo;
    s = S-e;
    t = T-f;
    s = (yhi-e)+(hi-s);
    t = (ylo-f)+(lo-t);
    e = s+T; H = S+e; h = e+(S-H); e = t+h;

    double zhi = H + e;
    double zlo = e + (H - zhi);
    hi = zhi;
    lo = zlo;
    return;
}

/* public */
void DD::selfAdd(double y)
{
    double H, h, S, s, e, f;
    S = hi + y;
    e = S - hi;
    s = S - e;
    s = (y - e) + (hi - s);
    f = s + lo;
    H = S + f;
    h = f + (S - H);
    hi = H + h;
    lo = h + (H - hi);
    return;
}

/* public */
DD operator+(const DD &lhs, const DD &rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfAdd(rhs);
    return rv;
}

/* public */
DD operator+(const DD &lhs, double rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfAdd(rhs);
    return rv;
}

/* public */
void DD::selfSubtract(const DD &d)
{
    return selfAdd(-1*d.hi, -1*d.lo);
}

/* public */
void DD::selfSubtract(double p_hi, double p_lo)
{
    return selfAdd(-1*p_hi, -1*p_lo);
}

/* public */
void DD::selfSubtract(double y)
{
    return selfAdd(-1*y, 0.0);
}

/* public */
DD operator-(const DD &lhs, const DD &rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfSubtract(rhs);
    return rv;
}

/* public */
DD operator-(const DD &lhs, double rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfSubtract(rhs);
    return rv;
}

/* public */
void DD::selfMultiply(double yhi, double ylo)
{
    double hx, tx, hy, ty, C, c;
    C = SPLIT * hi; hx = C-hi; c = SPLIT * yhi;
    hx = C-hx; tx = hi-hx; hy = c-yhi;
    C = hi*yhi; hy = c-hy; ty = yhi-hy;
    c = ((((hx*hy-C)+hx*ty)+tx*hy)+tx*ty)+(hi*ylo+lo*yhi);
    double zhi = C+c; hx = C-zhi;
    double zlo = c+hx;
    hi = zhi;
    lo = zlo;
    return;
}

/* public */
void DD::selfMultiply(DD const &d)
{
    return selfMultiply(d.hi, d.lo);
}

/* public */
void DD::selfMultiply(double y)
{
    return selfMultiply(y, 0.0);
}

/* public */
DD operator*(const DD &lhs, const DD &rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfMultiply(rhs);
    return rv;
}

/* public */
DD operator*(const DD &lhs, double rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfMultiply(rhs);
    return rv;
}


/* public */
void DD::selfDivide(double yhi, double ylo)
{
    double hc, tc, hy, ty, C, c, U, u;
    C = hi/yhi; c = SPLIT*C; hc =c-C;
    u = SPLIT*yhi; hc = c-hc;
    tc = C-hc; hy = u-yhi; U = C * yhi;
    hy = u-hy; ty = yhi-hy;
    u = (((hc*hy-U)+hc*ty)+tc*hy)+tc*ty;
    c = ((((hi-U)-u)+lo)-C*ylo)/yhi;
    u = C+c;
    hi = u;
    lo = (C-u)+c;
    return;
}

/* public */
void DD::selfDivide(const DD &d)
{
    return selfDivide(d.hi, d.lo);
}

/* public */
void DD::selfDivide(double y)
{
    return selfDivide(y, 0.0);
}

/* public */
DD operator/(const DD &lhs, const DD &rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfDivide(rhs);
    return rv;
}

/* public */
DD operator/(const DD &lhs, double rhs)
{
    DD rv(lhs.hi, lhs.lo);
    rv.selfDivide(rhs);
    return rv;
}

/* public */
DD DD::negate() const
{
    DD rv(hi, lo);
    if (rv.isNaN())
    {
        return rv;
    }
    rv.hi = -hi;
    rv.lo = -lo;
    return rv;
}

/* public static */
DD DD::reciprocal() const
{
    double  hc, tc, hy, ty, C, c, U, u;
    C = 1.0/hi;
    c = SPLIT*C;
    hc = c-C;
    u = SPLIT*hi;
    hc = c-hc; tc = C-hc; hy = u-hi; U = C*hi; hy = u-hy; ty = hi-hy;
    u = (((hc*hy-U)+hc*ty)+tc*hy)+tc*ty;
    c = ((((1.0-U)-u))-C*lo)/hi;
    double zhi = C+c;
    double zlo = (C-zhi)+c;
    return DD(zhi, zlo);
}

DD DD::floor() const
{
    DD rv(hi, lo);
    if (isNaN()) return rv;
    double fhi = std::floor(hi);
    double flo = 0.0;
    // Hi is already integral.  Floor the low word
    if (fhi == hi) {
      flo = std::floor(lo);
    }
      // do we need to renormalize here?
    rv.hi = fhi;
    rv.lo = flo;
    return rv;
}

DD DD::ceil() const
{
    DD rv(hi, lo);
    if (isNaN()) return rv;
    double fhi = std::ceil(hi);
    double flo = 0.0;
    // Hi is already integral.  Ceil the low word
    if (fhi == hi) {
      flo = std::ceil(lo);
      // do we need to renormalize here?
    }
    rv.hi = fhi;
    rv.lo = flo;
    return rv;
}

int DD::signum() const
{
    if (hi > 0) return 1;
    if (hi < 0) return -1;
    if (lo > 0) return 1;
    if (lo < 0) return -1;
    return 0;
}

DD DD::rint() const
{
    DD rv(hi, lo);
    if (isNaN()) return rv;
     return (rv + 0.5).floor();
}

/* public static */
DD DD::trunc(const DD &d)
{
    DD rv(d);
    if (rv.isNaN()) return rv;
    if (rv.isPositive())
        return rv.floor();
    return rv.ceil();
}

/* public static */
DD DD::abs(const DD &d)
{
    DD rv(d);
    if (rv.isNaN()) return rv;
    if (rv.isNegative())
        return rv.negate();

    return rv;
}

/* public static */
DD DD::determinant(const DD &x1, const DD &y1, const DD &x2, const DD &y2)
{
    return (x1 * y2) - (y1 * x2);
}

/* public static */
DD DD::determinant(double x1, double y1, double x2, double y2)
{
    return determinant(DD(x1), DD(y1), DD(x2), DD(y2) );
}

/**
* Computes the value of this number raised to an integral power.
* Follows semantics of Java Math.pow as closely as possible.
*/
/* public static */
DD DD::pow(const DD &d, int exp)
{
    if (exp == 0.0)
        return DD(1.0);

    DD r(d);
    DD s(1.0);
    int n = std::abs(exp);

    if (n > 1) {
        /* Use binary exponentiation */
        while (n > 0) {
        if (n % 2 == 1) {
            s.selfMultiply(r);
        }
        n /= 2;
        if (n > 0)
            r = r*r;
        }
    } else {
        s = r;
    }

    /* Compute the reciprocal if n is negative. */
    if (exp < 0)
        return s.reciprocal();
    return s;
}


}
}
