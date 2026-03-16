/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_ieeefloat.h"

/* See old implementation in src/deprecated/grib_ieeefloat.c */

constexpr auto ieee_table = IeeeTable<double>();

static void binary_search(const double xx[], const unsigned long n, double x, unsigned long* j)
{
    /*These routine works only on ascending ordered arrays*/
    unsigned long ju, jm, jl;
    jl = 0;
    ju = n;
    while (ju - jl > 1) {
        jm = (ju + jl) >> 1;
        /* printf("jl=%lu jm=%lu ju=%lu\n",jl,jm,ju); */
        /* printf("xx[jl]=%.10e xx[jm]=%.10e xx[ju]=%.10e\n",xx[jl],xx[jm],xx[ju]); */
        if (x >= xx[jm])
            jl = jm;
        else
            ju = jm;
    }
    *j = jl;
}

unsigned long grib_ieee_to_long(double x)
{
    unsigned long s    = 0;
    unsigned long mmax = 0xffffff;
    unsigned long mmin = 0x800000;
    unsigned long m    = 0;
    unsigned long e    = 0;
    double rmmax       = mmax + 0.5;

    /* printf("\ngrib_ieee_to_long: x=%.20e\n",x); */
    if (x < 0) {
        s = 1;
        x = -x;
    }

    /* Underflow */
    if (x < ieee_table.vmin) {
        /*printf("grib_ieee_to_long: (x < ieee_table.vmin) x=%.20e vmin=%.20e v=0x%lX\n",x,ieee_table.vmin,(s<<31));*/
        return (s << 31);
    }

    /* Overflow */
    if (x > ieee_table.vmax) {
        fprintf(stderr, "grib_ieee_to_long: Number is too large: x=%.20e > xmax=%.20e\n", x, ieee_table.vmax);
        ECCODES_ASSERT(0);
        return 0;
    }

    binary_search(ieee_table.v.data(), 254, x, &e);

    /* printf("grib_ieee_to_long: e=%ld\n",e); */

    x /= ieee_table.e[e];

    /* printf("grib_ieee_to_long: x=%.20e\n",x); */

    while (x < mmin) {
        x *= 2;
        e--;
        /* printf("grib_ieee_to_long (e--): x=%.20e e=%ld \n",x,e); */
    }

    while (x > rmmax) {
        x /= 2;
        e++;
        /* printf("grib_ieee_to_long (e++): x=%.20e e=%ld \n",x,e); */
    }

    m = x + 0.5;
    /* printf("grib_ieee_to_long: m=0x%lX (%lu) x=%.10e \n",m,m,x ); */
    if (m > mmax) {
        e++;
        m = 0x800000;
        /* printf("grib_ieee_to_long: ( m > mmax ) m=0x%lX (%lu) x=%.10e \n",m,m,x ); */
    }

    /* printf("grib_ieee_to_long: s=%lu c=%lu (0x%lX) m=%lu (0x%lX)\n",s,e,e,m,m ); */

    return (s << 31) | (e << 23) | (m & 0x7fffff);
}

double grib_ieeefloat_error(double x)
{
    unsigned long e = 0;

    if (x < 0)
        x = -x;

    /* Underflow */
    if (x < ieee_table.vmin)
        return ieee_table.vmin;

    /* Overflow */
    if (x > ieee_table.vmax) {
        fprintf(stderr, "grib_ieeefloat_error: Number is too large: x=%.20e > xmax=%.20e\n", x, ieee_table.vmax);
        ECCODES_ASSERT(0);
        return 0;
    }

    binary_search(ieee_table.v.data(), 254, x, &e);

    return ieee_table.e[e];
}

double grib_long_to_ieee(unsigned long x)
{
    unsigned long s = x & 0x80000000;
    unsigned long c = (x & 0x7f800000) >> 23;
    unsigned long m = (x & 0x007fffff);

    double val;

#ifdef DEBUG
    if (x > 0 && x < 0x800000) {
        fprintf(stderr, "grib_long_to_ieee: Invalid input %lu\n", x);
        ECCODES_ASSERT(0);
    }
#endif

    if (c == 0 && m == 0)
        return 0;

    if (c == 0) {
        m |= 0x800000;
        c = 1;
    }
    else
        m |= 0x800000;

    val = m * ieee_table.e[c];
    if (s)
        val = -val;

    return val;
}


unsigned long grib_ieee_nearest_smaller_to_long(double x)
{
    unsigned long l;
    unsigned long e;
    unsigned long m;
    unsigned long s;
    unsigned long mmin = 0x800000;
    double y, eps;

    if (x == 0)
        return 0;

    l = grib_ieee_to_long(x);
    y = grib_long_to_ieee(l);

    if (x < y) {
        if (x < 0 && -x < ieee_table.vmin) {
            l = 0x80800000;
        }
        else {
            e = (l & 0x7f800000) >> 23;
            m = (l & 0x007fffff) | 0x800000;
            s = l & 0x80000000;

            if (m == mmin) {
                /* printf("grib_ieee_nearest_smaller_to_long: m == mmin (0x%lX) e=%lu\n",m,e);  */
                e = s ? e : e - 1;
                if (e < 1)
                    e = 1;
                if (e > 254)
                    e = 254;
                /* printf("grib_ieee_nearest_smaller_to_long: e=%lu \n",e);  */
            }

            eps = ieee_table.e[e];

            /* printf("grib_ieee_nearest_smaller_to_long: x<y\n"); */
            l = grib_ieee_to_long(y - eps);
            /* printf("grib_ieee_nearest_smaller_to_long: grib_ieee_to_long(y-eps)=0x%lX y=%.10e eps=%.10e x=%.10e\n",l,y,eps,x); */
        }
    }
    else
        return l;

    if (x < grib_long_to_ieee(l)) {
        printf("grib_ieee_nearest_smaller_to_long: x=%.20e grib_long_to_ieee(0x%lX)=%.20e\n", x, l, grib_long_to_ieee(l));
        ECCODES_ASSERT(x >= grib_long_to_ieee(l));
    }

    return l;
}

int grib_nearest_smaller_ieee_float(double a, double* ret)
{
    unsigned long l = 0;

    if (a > ieee_table.vmax) {
        const grib_context* c = grib_context_get_default();
        grib_context_log(c, GRIB_LOG_ERROR,
                "Number is too large: x=%e > xmax=%e (IEEE float)", a, ieee_table.vmax);
        return GRIB_INTERNAL_ERROR;
    }

    l    = grib_ieee_nearest_smaller_to_long(a);
    *ret = grib_long_to_ieee(l);
    return GRIB_SUCCESS;
}

#ifdef IEEE


/*
 * To make these two routines consistent to grib_ieee_to_long and grib_long_to_ieee,
 * we should not do any byte swapping but rather perform a raw copy.
 * Byte swapping is actually implemented in grib_decode_unsigned_long and
 * grib_encode_unsigned_long.
 */
unsigned long grib_ieee64_to_long(double x)
{
    unsigned long lval;
    DEBUG_ASSERT(sizeof(double) == sizeof(long));
    memcpy(&lval, &x, sizeof(long));
    return lval;
}

double grib_long_to_ieee64(unsigned long x)
{
    double dval;
    DEBUG_ASSERT(sizeof(double) == sizeof(long));
    memcpy(&dval, &x, sizeof(long));
    return dval;
}

template <>
int grib_ieee_decode_array<double> (grib_context* c, unsigned char* buf, size_t nvals, int bytes, double* val)
{
    int err = 0, i = 0, j = 0;
    unsigned char s[8] = {0,};
    float fval;
    double* pval = val;

    switch (bytes) {
        case 4:
            for (i = 0; i < nvals; i++) {
#if IEEE_LE
                for (j = 3; j >= 0; j--)
                    s[j] = *(buf++);
                memcpy(&fval, s, 4);
                val[i] = (double)fval;
#elif IEEE_BE
                memcpy(&fval, buf, 4);
                val[i] = (double)fval;
                buf += 4;
#endif
            }
            break;
        case 8:
            for (i = 0; i < nvals; i++) {
#if IEEE_LE
                for (j = 7; j >= 0; j--)
                    s[j] = *(buf++);
                memcpy(pval++, s, 8);
#elif IEEE_BE
                memcpy(pval++, buf, 8);
                buf += 8;
#endif
            }
            break;
        default:
            grib_context_log(c, GRIB_LOG_ERROR,
                             "grib_ieee_decode_array: %d bits not implemented", bytes * 8);
            return GRIB_NOT_IMPLEMENTED;
    }

    return err;
}

template <>
int grib_ieee_decode_array<float>(grib_context* c, unsigned char* buf, size_t nvals, int bytes, float* val)
{
    int err = 0, i = 0, j = 0;
    unsigned char s[4] = {0,};

    switch (bytes) {
        case 4:
            for (i = 0; i < nvals; i++) {
#if IEEE_LE
                for (j = 3; j >= 0; j--)
                    s[j] = *(buf++);
                memcpy(&val[i], s, 4);
#elif IEEE_BE
                memcpy(&val[i], buf, 4);
                buf += 4;
#endif
            }
            break;
        default:
            grib_context_log(c, GRIB_LOG_ERROR,
                             "grib_ieee_decode_array_float: %d bits not implemented", bytes * 8);
            return GRIB_NOT_IMPLEMENTED;
    }

    return err;
}

#else

int grib_ieee_decode_array(grib_context* c, unsigned char* buf, size_t nvals, int bytes, double* val)
{
    int err = 0, i = 0;
    long bitr = 0;

    for (i = 0; i < nvals; i++)
        val[i] = grib_long_to_ieee(grib_decode_unsigned_long(buf, &bitr, bytes * 8));

    return err;
}

int grib_ieee_decode_array_float(grib_context* c, unsigned char* buf, size_t nvals, int bytes, float* val)
{
    int err = 0, i = 0;
    long bitr = 0;

    for (i = 0; i < nvals; i++)
        val[i] = (float) grib_long_to_ieee(grib_decode_unsigned_long(buf, &bitr, bytes * 8));

    return err;
}

#endif

#ifdef IEEE

int grib_ieee_encode_array(grib_context* c, double* val, size_t nvals, int bytes,
                           unsigned char* buf)
{
    int err = 0, i = 0, j = 0;
#if IEEE_LE
    unsigned char s4[4];
    unsigned char s8[8];
#endif
    float fval   = 0;
    double* pval = val;

    switch (bytes) {
        case 4:
            for (i = 0; i < nvals; i++) {
                fval = (float)val[i];

#if IEEE_LE
                memcpy(s4, &(fval), 4);
                for (j = 3; j >= 0; j--)
                    *(buf++) = s4[j];
#elif IEEE_BE
                memcpy(buf, &(fval), 4);
                buf += 4;
#endif
            }
            break;
        case 8:
            for (i = 0; i < nvals; i++) {
#if IEEE_LE
                memcpy(s8, pval++, 8);
                for (j = 7; j >= 0; j--)
                    *(buf++) = s8[j];
#elif IEEE_BE
                memcpy(buf, pval++, 8);
                buf += 8;
#endif
            }
            break;
        default:
            grib_context_log(c, GRIB_LOG_ERROR,
                             "grib_ieee_encode_array: %d bits not implemented", bytes * 8);
            return GRIB_NOT_IMPLEMENTED;
    }

    return err;
}

#else

int grib_ieee_encode_array(grib_context* c, double* val, size_t nvals, int bytes, unsigned char* buf)
{
    int err = 0, i = 0;
    long bitr = 0;

    for (i = 0; i < nvals; i++)
        grib_encode_unsigned_long(buf, grib_ieee_to_long(val[i]), &bitr, bytes * 8);

    return err;
}

#endif
