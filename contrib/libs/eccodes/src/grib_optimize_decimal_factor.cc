/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_scaling.h"
#include "grib_api_internal.h"
#include "grib_optimize_decimal_factor.h"
#include <cmath>
#include <float.h>
#include <string.h>

static double epsilon()
{
    double e = 1.;
    while (1. != (1. + e)) {
        e /= 2;
    }
    return e;
}

static int vrange()
{
    return (int)(log(DBL_MAX) / log(10)) - 1;
}

#ifdef ECCODES_ON_WINDOWS
#define log2(a) (log(a) / 1.44269504088896340736)
#endif

static void factec(int* krep, const double pa, const int knbit, const long kdec, const int range, long* ke, int* knutil)
{
    *krep = 0;

    *ke     = 0;
    *knutil = 0;

    if (pa < DBL_MIN) {
        *knutil = 1;
        goto end;
    }

    if ((fabs(log10(fabs(pa)) + (double)kdec) >= range)) {
        *krep = 1;
        goto end;
    }

    /* Binary scale factor associated to kdec */
    *ke = floor(log2((pa * codes_power<double>(kdec, 10)) / (codes_power<double>(knbit, 2) - 0.5))) + 1;
    /* Encoded value for pa = max - min       */
    *knutil = floor(0.5 + pa * codes_power<double>(kdec, 10) * codes_power<double>(-*ke, 2));

end:
    return;
}

int grib_optimize_decimal_factor(grib_accessor* a, const char* reference_value,
                                 const double pmax, const double pmin, const int knbit,
                                 const int compat_gribex, const int compat_32bit,
                                 long* kdec, long* kbin, double* ref)
{
    grib_handle* gh = grib_handle_of_accessor(a);
    int idecmin     = -15;
    int idecmax     = 5;
    long inbint;
    double xtinyr4, xhuger4, xnbint;
    int inumax, inutil;
    long jdec, ie;
    int irep;
    int RANGE      = vrange();
    double EPSILON = epsilon();
    double pa      = pmax - pmin;

    if (pa == 0) {
        *kdec = 0;
        *kbin = 0;
        *ref  = 0.;
        return GRIB_SUCCESS;
    }

    inumax = 0;

    if (fabs(pa) <= EPSILON) {
        *kdec   = 0;
        idecmin = 1;
        idecmax = 0;
    }
    else if (pmin != 0. && fabs(pmin) < EPSILON) {
        *kdec   = 0;
        idecmin = 1;
        idecmax = 0;
    }

    xtinyr4 = FLT_MIN;
    xhuger4 = FLT_MAX;

    inbint = codes_power<double>(knbit, 2) - 1;
    xnbint = (double)inbint;

    /* Test decimal scale factors; keep the most suitable */
    for (jdec = idecmin; jdec <= idecmax; jdec++) {
        /* Fix a problem in GRIBEX */
        if (compat_gribex)
            if (pa * codes_power<double>(jdec, 10) <= 1.E-12)
                continue;

        /* Check it will be possible to decode reference value with 32bit floats */
        if (compat_32bit)
            if (fabs(pmin) > DBL_MIN)
                if (log10(fabs(pmin)) + (double)jdec <= log10(xtinyr4))
                    continue;

        /* Check if encoding will not cause an overflow */
        if (fabs(log10(fabs(pa)) + (double)jdec) >= (double)RANGE)
            continue;

        factec(&irep, pa, knbit, jdec, RANGE, &ie, &inutil);

        if (irep != 0)
            continue;

        /* Check it will be possible to decode the maximum value of the fields using 32bit floats */
        if (compat_32bit)
            if (pmin * codes_power<double>(jdec, 10) + xnbint * codes_power<double>(ie, 2) >= xhuger4)
                continue;

        /* GRIB1 demands that the binary scale factor be encoded in a single byte */
        if (compat_gribex)
            if ((ie < -126) || (ie > 127))
                continue;

        if (inutil > inumax) {
            inumax = inutil;
            *kdec  = jdec;
            *kbin  = ie;
        }
    }

    if (inumax > 0) {
        double decimal = codes_power<double>(+*kdec, 10);
        double divisor = codes_power<double>(-*kbin, 2);
        double min     = pmin * decimal;
        long vmin, vmax;
        if (grib_get_nearest_smaller_value(gh, reference_value, min, ref) != GRIB_SUCCESS) {
            grib_context_log(gh->context, GRIB_LOG_ERROR,
                             "Unable to find nearest_smaller_value of %g for %s", min, reference_value);
            return GRIB_INTERNAL_ERROR;
        }

        vmax = (((pmax * decimal) - *ref) * divisor) + 0.5;
        vmin = (((pmin * decimal) - *ref) * divisor) + 0.5;

        /* This may happen if pmin*decimal-*ref is too large */
        if ((vmin != 0) || (vmax > inbint))
            inumax = 0;
    }

    /* If seeking for an optimal decimal scale factor fails, fall back to a basic method */
    if (inumax == 0) {
        int last   = compat_gribex ? 99 : 127;
        double min = pmin, max = pmax;
        double range    = max - min;
        double f        = codes_power<double>(knbit, 2) - 1;
        double minrange = codes_power<double>(-last, 2) * f;
        double maxrange = codes_power<double>(+last, 2) * f;
        double decimal  = 1;
        int err;

        *kdec = 0;

        while (range < minrange) {
            *kdec += 1;
            decimal *= 10;
            min   = pmin * decimal;
            max   = pmax * decimal;
            range = max - min;
        }

        while (range > maxrange) {
            *kdec -= 1;
            decimal /= 10;
            min   = pmin * decimal;
            max   = pmax * decimal;
            range = max - min;
        }

        if (grib_get_nearest_smaller_value(gh, reference_value, min, ref) != GRIB_SUCCESS) {
            grib_context_log(gh->context, GRIB_LOG_ERROR,
                             "Unable to find nearest_smaller_value of %g for %s", min, reference_value);
            return GRIB_INTERNAL_ERROR;
        }

        *kbin = grib_get_binary_scale_fact(max, *ref, knbit, &err);

        if (err == GRIB_UNDERFLOW) {
            *kbin = 0;
            *kdec = 0;
            *ref  = 0;
        }
    }

    return GRIB_SUCCESS;
}
