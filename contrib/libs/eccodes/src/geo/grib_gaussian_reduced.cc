/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */
#include "grib_api_internal.h"


/*
 * C Implementation: gaussian_reduced
 *
 * Description: computes the number of points within the range
 *              lon_first->lon_last and the zero based indexes
 *              ilon_first,ilon_last of the first and last point
 *              given the number of points along a parallel (pl)
 *
 */
#define EFDEBUG 0

#ifndef LLONG_MAX
#define LLONG_MAX LONG_MAX
#endif
#ifndef ULLONG_MAX
#define ULLONG_MAX ULONG_MAX
#endif


typedef long long Fraction_value_type;

typedef struct Fraction_type
{
    Fraction_value_type top_;
    Fraction_value_type bottom_;
} Fraction_type;

const Fraction_value_type MAX_DENOM = 3037000499; /* sqrt(LLONG_MAX) */

static Fraction_value_type fraction_gcd(Fraction_value_type a, Fraction_value_type b)
{
    while (b != 0) {
        Fraction_value_type r = a % b;
        a                     = b;
        b                     = r;
    }
    return a;
}
static Fraction_type fraction_construct(Fraction_value_type top, Fraction_value_type bottom)
{
    Fraction_type result;

    /* @note in theory we also assume that numerator and denominator are both representable in
     * double without loss
     *   ASSERT(top == Fraction_value_type(double(top)));
     *   ASSERT(bottom == Fraction_value_type(double(bottom)));
     */
    Fraction_value_type g;
    Fraction_value_type sign = 1;
    ECCODES_ASSERT(bottom != 0);
    if (top < 0) {
        top  = -top;
        sign = -sign;
    }

    if (bottom < 0) {
        bottom = -bottom;
        sign   = -sign;
    }

    g = fraction_gcd(top, bottom);
    if (g != 0) {
        top    = top / g;
        bottom = bottom / g;
    }

    result.top_    = sign * top;
    result.bottom_ = bottom;
    return result;
}

static Fraction_type fraction_construct_from_double(double x)
{
    Fraction_type result;
    double value             = x;
    Fraction_value_type sign = 1;
    Fraction_value_type m00 = 1, m11 = 1, m01 = 0, m10 = 0;
    Fraction_value_type a = x;
    Fraction_value_type t2, top, bottom, g;
    size_t cnt = 0;

    /*ECCODES_ASSERT(x != NAN);*/
    ECCODES_ASSERT(fabs(x) < 1e30);

    if (x < 0) {
        sign = -sign;
        x    = -x;
    }

    t2 = m10 * a + m11;

    while (t2 <= MAX_DENOM) {
        Fraction_value_type t1 = m00 * a + m01;
        m01                    = m00;
        m00                    = t1;

        m11 = m10;
        m10 = t2;

        if (x == a) {
            break;
        }

        x = 1.0 / (x - a);

        if (x > (double)LLONG_MAX) {
            break;
        }

        a  = x;
        t2 = m10 * a + m11;

        if (cnt++ > 10000) {
            fprintf(stderr, "Cannot compute fraction from %g\n", value);
        }
    }

    while (m10 >= MAX_DENOM || m00 >= MAX_DENOM) {
        m00 >>= 1;
        m10 >>= 1;
    }

    top    = m00;
    bottom = m10;

    g      = fraction_gcd(top, bottom);
    top    = top / g;
    bottom = bottom / g;

    result.top_    = sign * top;
    result.bottom_ = bottom;
    return result;
}

static Fraction_value_type fraction_integralPart(const Fraction_type frac)
{
    ECCODES_ASSERT(frac.bottom_);
    if (frac.bottom_ == 0) return frac.top_;
    return frac.top_ / frac.bottom_;
}

/*static int fraction_operator_equality(Fraction_type self, Fraction_type other)
{
    return self.top_ == other.top_ && self.bottom_ == other.bottom_;
}*/

static double fraction_operator_double(Fraction_type self)
{
    return (double)self.top_ / (double)self.bottom_;
}

static Fraction_value_type fraction_mul(int* overflow, Fraction_value_type a, Fraction_value_type b)
{
    if (*overflow) {
        return 0;
    }

    if (b != 0) {
        *overflow = llabs(a) > (ULLONG_MAX / llabs(b));
    }
    return a * b;
}

/* Fraction Fraction::operator/(const Fraction& other) */
static Fraction_type fraction_operator_divide(Fraction_type self, Fraction_type other)
{
    int overflow = 0; /*boolean*/

    Fraction_value_type top    = fraction_mul(&overflow, self.top_, other.bottom_);
    Fraction_value_type bottom = fraction_mul(&overflow, self.bottom_, other.top_);

    if (!overflow) {
        return fraction_construct(top, bottom);
    }
    else {
        /*Fallback option*/
        /*return Fraction(double(*this) / double(other));*/
        double d1        = fraction_operator_double(self);
        double d2        = fraction_operator_double(other);
        Fraction_type f1 = fraction_construct_from_double(d1 / d2);
        return f1;
    }
}

/* Fraction Fraction::operator*(const Fraction& other) */
static Fraction_type fraction_operator_multiply(Fraction_type self, Fraction_type other)
{
    int overflow = 0; /*boolean*/

    Fraction_value_type top    = fraction_mul(&overflow, self.top_, other.top_);
    Fraction_value_type bottom = fraction_mul(&overflow, self.bottom_, other.bottom_);

    if (!overflow) {
        return fraction_construct(top, bottom);
    }
    else {
        /* Fallback option */
        /*return Fraction(double(*this) * double(other));*/
        double d1        = fraction_operator_double(self);
        double d2        = fraction_operator_double(other);
        Fraction_type f1 = fraction_construct_from_double(d1 * d2);
        return f1;
    }
}

/* bool Fraction::operator<(const Fraction& other) */
static int fraction_operator_less_than(Fraction_type self, Fraction_type other)
{
    int overflow = 0;
    int result   = fraction_mul(&overflow, self.top_, other.bottom_) < fraction_mul(&overflow, other.top_, self.bottom_);
    if (overflow) {
        double d1 = fraction_operator_double(self);
        double d2 = fraction_operator_double(other);
        return (d1 < d2);
        /* return double(*this) < double(other); */
    }
    return result;
}

/* bool Fraction::operator>(const Fraction& other) */
static int fraction_operator_greater_than(Fraction_type self, Fraction_type other)
{
    int overflow = 0;
    int result   = fraction_mul(&overflow, self.top_, other.bottom_) > fraction_mul(&overflow, other.top_, self.bottom_);
    if (overflow) {
        double d1 = fraction_operator_double(self);
        double d2 = fraction_operator_double(other);
        return (d1 > d2);
        /* return double(*this) > double(other);*/
    }
    return result;
}

/*explicit Fraction(long long n): top_(n), bottom_(1) {}*/
static Fraction_type fraction_construct_from_long_long(long long n)
{
    Fraction_type result;
    result.top_    = n;
    result.bottom_ = 1;
    return result;
}

/*template<class T>Fraction operator*(T n, const Fraction& f){    return Fraction(n) * f;  }*/
static Fraction_type fraction_operator_multiply_n_Frac(Fraction_value_type n, Fraction_type f)
{
    Fraction_type ft     = fraction_construct_from_long_long(n);
    Fraction_type result = fraction_operator_multiply(ft, f);
    return result;
    /*return Fraction(n) * f;*/
}

static Fraction_value_type get_min(Fraction_value_type a, Fraction_value_type b)
{
    return ((a < b) ? a : b);
}

static void gaussian_reduced_row(
    long long Ni_globe,    /*plj*/
    const Fraction_type w, /*lon_first*/
    const Fraction_type e, /*lon_last*/
    long long* pNi,        /*npoints*/
    double* pLon1,
    double* pLon2)
{
    Fraction_value_type Nw, Ne;
    Fraction_type inc, Nw_inc, Ne_inc;
    inc = fraction_construct(360ll, Ni_globe);

    /* auto Nw = (w / inc).integralPart(); */
    Nw     = fraction_integralPart(fraction_operator_divide(w, inc));
    Nw_inc = fraction_operator_multiply_n_Frac(Nw, inc);

    ECCODES_ASSERT(Ni_globe > 1);
    /*if (Nw * inc < w) {*/
    if (fraction_operator_less_than(Nw_inc, w)) {
        Nw += 1;
    }

    /*auto Ne = (e / inc).integralPart();*/
    Ne     = fraction_integralPart(fraction_operator_divide(e, inc));
    Ne_inc = fraction_operator_multiply_n_Frac(Ne, inc);
    /* if (Ne * inc > e) */
    if (fraction_operator_greater_than(Ne_inc, e)) {
        Ne -= 1;
    }
    if (Nw > Ne) {
        *pNi   = 0;          /* no points on this latitude */
        *pLon1 = *pLon2 = 0; /* dummy - unused */
    }
    else {
        *pNi = get_min(Ni_globe, Ne - Nw + 1);

        Nw_inc = fraction_operator_multiply_n_Frac(Nw, inc);
        *pLon1 = fraction_operator_double(Nw_inc);
        Ne_inc = fraction_operator_multiply_n_Frac(Ne, inc);
        *pLon2 = fraction_operator_double(Ne_inc);
    }
}

/* --------------------------------------------------------------------------------------------------- */
void grib_get_reduced_row_wrapper(grib_handle* h, long pl, double lon_first, double lon_last, long* npoints, long* ilon_first, long* ilon_last)
{
    grib_get_reduced_row(pl, lon_first, lon_last, npoints, ilon_first, ilon_last);

    /* Legacy
     * grib_get_reduced_row1(pl, lon_first, lon_last, npoints, ilon_first, ilon_last);
     */
}

/* This was the legacy way of counting the points within a subarea of a Gaussian grid.
   In the days of Prodgen/libemos */
void grib_get_reduced_row_legacy(long pl, double lon_first, double lon_last, long* npoints, long* ilon_first, long* ilon_last)
{
    double range = 0, dlon_first = 0, dlon_last = 0;
    long irange;
    range = lon_last - lon_first;
    if (range < 0) {
        range += 360;
        lon_first -= 360;
    }

    /* computing integer number of points and coordinates without using floating point resolution*/
    *npoints    = (range * pl) / 360.0 + 1;
    *ilon_first = (lon_first * pl) / 360.0;
    *ilon_last  = (lon_last * pl) / 360.0;

    irange = *ilon_last - *ilon_first + 1;

#if EFDEBUG
    printf("  pl=%ld npoints=%ld range=%.10e ilon_first=%ld ilon_last=%ld irange=%ld\n",
           pl, *npoints, range, *ilon_first, *ilon_last, irange);
#endif

    if (irange != *npoints) {
#if EFDEBUG
        printf("       ---> (irange=%ld) != (npoints=%ld) ", irange, *npoints);
#endif
        if (irange > *npoints) {
            /* checking if the first point is out of range*/
            dlon_first = ((*ilon_first) * 360.0) / pl;
            if (dlon_first < lon_first) {
                (*ilon_first)++;
                irange--;
#if EFDEBUG
                printf(" dlon_first=%.10e < lon_first=%.10e\n", dlon_first, lon_first);
#endif
            }

            /* checking if the last point is out of range*/
            dlon_last = ((*ilon_last) * 360.0) / pl;
            if (dlon_last > lon_last) {
                (*ilon_last)--;
                irange--;
#if EFDEBUG
                printf(" dlon_last=%.10e < lon_last=%.10e\n", dlon_last, lon_last);
#endif
            }
        }
        else {
            int ok = 0;
            /* checking if the point before the first is in the range*/
            dlon_first = ((*ilon_first - 1) * 360.0) / pl;
            if (dlon_first > lon_first) {
                (*ilon_first)--;
                irange++;
                ok = 1;
#if EFDEBUG
                printf(" dlon_first1=%.10e > lon_first=%.10e\n", dlon_first, lon_first);
#endif
            }

            /* checking if the point after the last is in the range*/
            dlon_last = ((*ilon_last + 1) * 360.0) / pl;
            if (dlon_last < lon_last) {
                (*ilon_last)++;
                irange++;
                ok = 1;
#if EFDEBUG
                printf(" dlon_last1=%.10e > lon_last=%.10e\n", dlon_last, lon_first);
#endif
            }

            /* if neither of the two are triggered then npoints is too large */
            if (!ok) {
                (*npoints)--;
#if EFDEBUG
                printf(" (*npoints)--=%ld\n", *npoints);
#endif
            }
        }

        /*ECCODES_ASSERT(*npoints==irange);*/
#if EFDEBUG
        printf("--  pl=%ld npoints=%ld range=%.10e ilon_first=%ld ilon_last=%ld irange=%ld\n",
               pl, *npoints, range, *ilon_first, *ilon_last, irange);
#endif
    }
    else {
        /* checking if the first point is out of range*/
        dlon_first = ((*ilon_first) * 360.0) / pl;
        if (dlon_first < lon_first) {
            (*ilon_first)++;
            (*ilon_last)++;
#if EFDEBUG
            printf("       ---> dlon_first=%.10e < lon_first=%.10e\n", dlon_first, lon_first);
            printf("--  pl=%ld npoints=%ld range=%.10e ilon_first=%ld ilon_last=%ld irange=%ld\n",
                   pl, *npoints, range, *ilon_first, *ilon_last, irange);
#endif
        }
    }

    if (*ilon_first < 0)
        *ilon_first += pl;

    return;
}

/* New method based on eckit Fractions and matching MIR count */
void grib_get_reduced_row(long pl, double lon_first, double lon_last, long* npoints, long* ilon_first, long* ilon_last)
{
    long long Ni_globe = pl;
    Fraction_type west;
    Fraction_type east;
    long long the_count;
    double the_lon1, the_lon2;

    while (lon_last < lon_first)
        lon_last += 360;
    west = fraction_construct_from_double(lon_first);
    east = fraction_construct_from_double(lon_last);

    gaussian_reduced_row(
        Ni_globe, /*plj*/
        west,     /*lon_first*/
        east,     /*lon_last*/
        &the_count,
        &the_lon1,
        &the_lon2);
    *npoints    = (long)the_count;
    *ilon_first = (the_lon1 * pl) / 360.0;
    *ilon_last  = (the_lon2 * pl) / 360.0;
}

/* This version returns the actual first and last longitudes rather than indexes */
void grib_get_reduced_row_p(long pl, double lon_first, double lon_last, long* npoints, double* olon_first, double* olon_last)
{
    long long Ni_globe = pl;
    Fraction_type west;
    Fraction_type east;
    long long the_count;
    double the_lon1, the_lon2;

    while (lon_last < lon_first)
        lon_last += 360;
    west = fraction_construct_from_double(lon_first);
    east = fraction_construct_from_double(lon_last);

    gaussian_reduced_row(
        Ni_globe, /*plj*/
        west,     /*lon_first*/
        east,     /*lon_last*/
        &the_count,
        &the_lon1,
        &the_lon2);
    *npoints    = (long)the_count;
    *olon_first = the_lon1;
    *olon_last  = the_lon2;
}
