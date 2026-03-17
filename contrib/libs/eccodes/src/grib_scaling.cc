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
#include <math.h>

// Unfortunately, metkit uses grib_power() (illegal usage of private API)
// As soon as it is fixed, the wrapper below can be deleted
double grib_power(long s, long n) {
    return codes_power<double>(s, n);
}

long grib_get_binary_scale_fact(double max, double min, long bpval, int* error)
{
    double range         = max - min;
    double zs            = 1;
    long scale           = 0;
    const long last      = 127; /* Depends on edition, should be parameter */
    unsigned long maxint = 0;
    const size_t ulong_size = sizeof(maxint) * 8;

    // See ECC-1927
    if ((isnan(range) || isinf(range))) {
        *error = GRIB_OUT_OF_RANGE; /*overflow*/
        return 0;
    }

    /* See ECC-246
      unsigned long maxint = codes_power<double>(bpval,2) - 1;
      double dmaxint=(double)maxint;
    */
    if (bpval >= ulong_size) {
        *error = GRIB_OUT_OF_RANGE; /*overflow*/
        return 0;
    }
    const double dmaxint = codes_power<double>(bpval, 2) - 1;
    maxint = (unsigned long)dmaxint; /* Now it's safe to cast */

    *error = 0;
    if (bpval < 1) {
        *error = GRIB_ENCODING_ERROR; /* constant field */
        return 0;
    }

    ECCODES_ASSERT(bpval >= 1);
    if (range == 0)
        return 0;

    /* range -= 1e-10; */
    while ((range * zs) <= dmaxint) {
        scale--;
        zs *= 2;
    }

    while ((range * zs) > dmaxint) {
        scale++;
        zs /= 2;
    }

    while ((unsigned long)(range * zs + 0.5) <= maxint) {
        scale--;
        zs *= 2;
    }

    while ((unsigned long)(range * zs + 0.5) > maxint) {
        scale++;
        zs /= 2;
    }

    if (scale < -last) {
        *error = GRIB_UNDERFLOW;
        scale = -last;
    }
    ECCODES_ASSERT(scale <= last);
    return scale;
}

// long grib_get_bits_per_value(double max, double min, long binary_scale_factor)
// {
//     double range    = max - min;
//     double zs       = 1;
//     long scale      = 0;
//     const long last = 127; /* Depends on edition, should be parameter */
//     unsigned long maxint = codes_power<double>(binary_scale_factor, 2) - 1;
//     double dmaxint       = (double)maxint;
//     if (maxint == 0)
//         maxint = 1;
//     /*  printf("---- Maxint %ld range=%g\n",maxint,range);     */
//     if (range == 0)
//         return 0;
//     /* range -= 1e-10; */
//     while ((range * zs) <= dmaxint) {
//         scale--;
//         zs *= 2;
//     }
//     while ((range * zs) > dmaxint) {
//         scale++;
//         zs /= 2;
//     }
//     while ((unsigned long)(range * zs + 0.5) <= maxint) {
//         scale--;
//         zs *= 2;
//     }
//     while ((unsigned long)(range * zs + 0.5) > maxint) {
//         scale++;
//         zs /= 2;
//     }
//     ECCODES_ASSERT(scale >= -last && scale <= last);
//     /* printf("---- scale=%ld\n",scale);*/
//     return scale;
// }

// long grib_get_decimal_scale_fact(double max, double min, long bpval, long binary_scale)
// {
//     double range    = max - min;
//     double zs       = 1;
//     long scale      = 0;
//     const long last = 127; /* Depends on edition, should be parameter */
//     unsigned long maxint = codes_power<double>(bpval, 2) - 1;
//     double dmaxint       = (double)maxint;
//     range *= codes_power<double>(-binary_scale, 2);
//     ECCODES_ASSERT(bpval >= 1);
//     if (range == 0)
//         return 0;
//     while ((range * zs) > dmaxint) {
//         scale--;
//         zs /= 10;
//     }
//     while ((range * zs) <= dmaxint) {
//         scale++;
//         zs *= 10;
//     }
//     while ((unsigned long)(range * zs + 0.5) > maxint) {
//         scale--;
//         zs /= 10;
//     }
//     while ((unsigned long)(range * zs + 0.5) <= maxint) {
//         scale++;
//         zs *= 10;
//     }
//     /* printf("grib_api: decimal_scale_fact=%ld max=%g min=%g bits_per_value=%ld binary_scale=%ld\n",scale,max,min,bpval,binary_scale); */
//     ECCODES_ASSERT(scale >= -last && scale <= last);
//     return scale;
// }
