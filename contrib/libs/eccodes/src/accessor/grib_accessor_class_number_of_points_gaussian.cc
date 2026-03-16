/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_number_of_points_gaussian.h"

grib_accessor_number_of_points_gaussian_t _grib_accessor_number_of_points_gaussian{};
grib_accessor* grib_accessor_number_of_points_gaussian = &_grib_accessor_number_of_points_gaussian;

#define EFDEBUG 0

void grib_accessor_number_of_points_gaussian_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n          = 0;
    grib_handle* h = grib_handle_of_accessor(this);

    ni_             = c->get_name(h, n++);
    nj_             = c->get_name(h, n++);
    plpresent_      = c->get_name(h, n++);
    pl_             = c->get_name(h, n++);
    order_          = c->get_name(h, n++);
    lat_first_      = c->get_name(h, n++);
    lon_first_      = c->get_name(h, n++);
    lat_last_       = c->get_name(h, n++);
    lon_last_       = c->get_name(h, n++);
    support_legacy_ = c->get_name(h, n++);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    length_ = 0;
}

// Old implementation of num_points_reduced_gauss_old
// See src/deprecated/grib_accessor_number_of_points_gaussian.cc
//
static int angleApproximatelyEqual(double A, double B, double angular_precision)
{
    return angular_precision > 0 ? (fabs(A - B) <= angular_precision) : (A == B);
}

static double longitude_normalise(double lon, double minimum)
{
    while (lon < minimum) {
        lon += 360;
    }
    while (lon >= minimum + 360) {
        lon -= 360;
    }
    return lon;
}

static void correctWestEast(long max_pl, double angular_precision, double* pWest, double* pEast)
{
    double w, e;
    const double inc = 360.0 / max_pl; /*smallest increment*/
    if (*pWest > *pEast)
        *pEast += 360;

    w = *pWest;
    e = *pEast;

    if (angleApproximatelyEqual(0, w, angular_precision)) {
        const int cond1 = angleApproximatelyEqual(360 - inc, e - w, angular_precision);
        const int cond2 = (360 - inc < e - w);
        const int cond3 = (e != w);
        const int cond4 = longitude_normalise(e, w) == w; /* e.normalise(w) == w */
        if (cond1 || cond2 || (cond3 && cond4)) {
            *pWest = 0;
            *pEast = 360 - inc;
        }
    }
}

static int get_number_of_data_values(grib_handle* h, size_t* numDataValues)
{
    int err  = 0;
    long bpv = 0, bitmapPresent = 0;
    size_t bitmapLength = 0;

    if ((err = grib_get_long(h, "bitsPerValue", &bpv)))
        return err;

    if (bpv != 0) {
        if (grib_get_size(h, "values", numDataValues) == GRIB_SUCCESS) {
            return GRIB_SUCCESS;
        }
    }
    else {
        /* Constant field (with or without bitmap) */
        if ((err = grib_get_long(h, "bitmapPresent", &bitmapPresent)))
            return err;
        if (bitmapPresent) {
            if ((err = grib_get_size(h, "bitmap", &bitmapLength)))
                return err;
            *numDataValues = bitmapLength;
            return GRIB_SUCCESS;
        }
        else {
            err = GRIB_NO_VALUES; /* Cannot determine number of values */
        }
    }

    return err;
}

int grib_accessor_number_of_points_gaussian_t::unpack_long(long* val, size_t* len)
{
    int err             = GRIB_SUCCESS;
    long support_legacy = 1;
    grib_handle* h      = grib_handle_of_accessor(this);

    if ((err = grib_get_long_internal(h, support_legacy_, &support_legacy)) != GRIB_SUCCESS)
        return err;

    if (support_legacy == 1)
        return unpack_long_with_legacy_support(val, len);
    else
        return unpack_long_new(val, len);
}

/* New algorithm */
int grib_accessor_number_of_points_gaussian_t::unpack_long_new(long* val, size_t* len)
{
    int err                                         = GRIB_SUCCESS;
    int is_global                                   = 0;
    long ni = 0, nj = 0, plpresent = 0, order = 0;
    size_t plsize = 0;
    double lat_first, lat_last, lon_first, lon_last;
    long* pl     = NULL;
    long* plsave = NULL;
    long row_count;
    long ilon_first = 0, ilon_last = 0;
    double angular_precision = 1.0 / 1000000.0;
    long angleSubdivisions   = 0;
    grib_handle* h           = grib_handle_of_accessor(this);

    grib_context* c = context_;

    if ((err = grib_get_long_internal(h, ni_, &ni)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(h, nj_, &nj)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(h, plpresent_, &plpresent)) != GRIB_SUCCESS)
        return err;

    if (nj == 0)
        return GRIB_GEOCALCULUS_PROBLEM;

    if (grib_get_long(h, "angleSubdivisions", &angleSubdivisions) == GRIB_SUCCESS) {
        ECCODES_ASSERT(angleSubdivisions > 0);
        angular_precision = 1.0 / angleSubdivisions;
    }

    if (plpresent) {
        long max_pl = 0;
        int j       = 0;
        // double lon_first_row = 0, lon_last_row = 0;

        /*reduced*/
        if ((err = grib_get_long_internal(h, order_, &order)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lat_first_, &lat_first)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lon_first_, &lon_first)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lat_last_, &lat_last)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lon_last_, &lon_last)) != GRIB_SUCCESS)
            return err;

        if ((err = grib_get_size(h, pl_, &plsize)) != GRIB_SUCCESS)
            return err;

        pl     = (long*)grib_context_malloc_clear(c, sizeof(long) * plsize);
        plsave = pl;
        grib_get_long_array_internal(h, pl_, pl, &plsize);

        if (lon_last < 0)
            lon_last += 360;
        if (lon_first < 0)
            lon_first += 360;

        /* Find the maximum element of "pl" array, do not assume it's 4*N! */
        /* This could be an Octahedral Gaussian Grid */
        max_pl = pl[0];
        for (j = 1; j < plsize; j++) {
            if (pl[j] > max_pl)
                max_pl = pl[j];
        }

        is_global = 0; /* ECC-445 */

        correctWestEast(max_pl, angular_precision, &lon_first, &lon_last);

        if (!is_global) {
            /*sub area*/
            *val = 0;
            for (j = 0; j < nj; j++) {
                row_count = 0;
                if (pl[j] == 0) {
                    grib_context_log(h->context, GRIB_LOG_ERROR, "Invalid pl array: entry at index=%d is zero", j);
                    return GRIB_GEOCALCULUS_PROBLEM;
                }
                grib_get_reduced_row_wrapper(h, pl[j], lon_first, lon_last, &row_count, &ilon_first, &ilon_last);
                // lon_first_row = ((ilon_first)*360.0) / pl[j];
                // lon_last_row  = ((ilon_last)*360.0) / pl[j];
                *val += row_count;
            }
        }
        else {
            int i = 0;
            *val  = 0;
            for (i = 0; i < plsize; i++)
                *val += pl[i];
        }
    }
    else {
        /*regular*/
        *val = ni * nj;
    }
    if (plsave)
        grib_context_free(c, plsave);

    return err;
}

/* With Legacy support */
int grib_accessor_number_of_points_gaussian_t::unpack_long_with_legacy_support(long* val, size_t* len)
{
    int err                                         = GRIB_SUCCESS;
    int is_global                                   = 0;
    long ni = 0, nj = 0, plpresent = 0, order = 0;
    size_t plsize = 0;
    double lat_first, lat_last, lon_first, lon_last;
    long* pl     = NULL;
    long* plsave = NULL;
    long row_count;
    long ilon_first = 0, ilon_last = 0;
    double angular_precision = 1.0 / 1000000.0;
    long angleSubdivisions   = 0;
    grib_handle* h           = grib_handle_of_accessor(this);
    size_t numDataValues     = 0;

    grib_context* c = context_;

    if ((err = grib_get_long_internal(h, ni_, &ni)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(h, nj_, &nj)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(h, plpresent_, &plpresent)) != GRIB_SUCCESS)
        return err;

    if (nj == 0)
        return GRIB_GEOCALCULUS_PROBLEM;

    if (grib_get_long(h, "angleSubdivisions", &angleSubdivisions) == GRIB_SUCCESS) {
        ECCODES_ASSERT(angleSubdivisions > 0);
        angular_precision = 1.0 / angleSubdivisions;
    }

    if (plpresent) {
        long max_pl = 0;
        int j       = 0;
        // double lon_first_row = 0, lon_last_row = 0;

        /*reduced*/
        if ((err = grib_get_long_internal(h, order_, &order)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lat_first_, &lat_first)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lon_first_, &lon_first)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lat_last_, &lat_last)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_get_double_internal(h, lon_last_, &lon_last)) != GRIB_SUCCESS)
            return err;

        if ((err = grib_get_size(h, pl_, &plsize)) != GRIB_SUCCESS)
            return err;

        pl     = (long*)grib_context_malloc_clear(c, sizeof(long) * plsize);
        plsave = pl;
        grib_get_long_array_internal(h, pl_, pl, &plsize);

        if (lon_last < 0)
            lon_last += 360;
        if (lon_first < 0)
            lon_first += 360;

        /* Find the maximum element of "pl" array, do not assume it's 4*N! */
        /* This could be an Octahedral Gaussian Grid */
        max_pl = pl[0];
        for (j = 1; j < plsize; j++) {
            if (pl[j] > max_pl)
                max_pl = pl[j];
        }

        /*is_global=is_gaussian_global(lat_first,lat_last,lon_first,lon_last,max_pl,lats,angular_precision);*/
        is_global = 0; /* ECC-445 */

        correctWestEast(max_pl, angular_precision, &lon_first, &lon_last);

        if (!is_global) {
            /*sub area*/
#if EFDEBUG
            printf("-------- subarea fabs(lat_first-lats[0])=%g d=%g\n", fabs(lat_first - lats[0]), d);
            printf("-------- subarea fabs(lat_last+lats[0])=%g d=%g\n", fabs(lat_last + lats[0]), d);
            printf("-------- subarea lon_last=%g order=%ld 360.0-90.0/order=%g\n",
                   lon_last, order, 360.0 - 90.0 / order);
            printf("-------- subarea lon_first=%g fabs(lon_last  -( 360.0-90.0/order))=%g 90.0/order=%g\n",
                   lon_first, fabs(lon_last - (360.0 - 90.0 / order)), 90.0 / order);
#endif
            *val = 0;
            for (j = 0; j < nj; j++) {
                row_count = 0;
#if EFDEBUG
                printf("--  %d ", j);
#endif
                if (pl[j] == 0) {
                    grib_context_log(h->context, GRIB_LOG_ERROR, "Invalid pl array: entry at index=%d is zero", j);
                    return GRIB_GEOCALCULUS_PROBLEM;
                }
                grib_get_reduced_row_wrapper(h, pl[j], lon_first, lon_last, &row_count, &ilon_first, &ilon_last);

                //                 if ( row_count != pl[j] ) {
                //                     printf("oops...... rc=%ld but pl[%d]=%ld\n", row_count, j,pl[j]);
                //                 }
                // lon_first_row = ((ilon_first)*360.0) / pl[j];
                // lon_last_row  = ((ilon_last)*360.0) / pl[j];
                *val += row_count;
#if EFDEBUG
                printf("        ilon_first=%ld lon_first=%.10e ilon_last=%ld lon_last=%.10e count=%ld row_count=%ld\n",
                       ilon_first, lon_first_row, ilon_last, lon_last_row, *val, row_count);
#endif
            }
        }
        else {
            int i = 0;
            *val  = 0;
            for (i = 0; i < plsize; i++)
                *val += pl[i];
        }
    }
    else {
        /*regular*/
        *val = ni * nj;
    }
#if EFDEBUG
    printf("DEBUG:     number_of_points_gaussian=%ld plpresent=%ld plsize=%d\n", *val, plpresent, plsize);
    for (i = 0; i < plsize; i++)
        printf(" DEBUG: pl[%d]=%ld\n", i, pl[i]);
#endif

    if (plsave)
        grib_context_free(c, plsave);

    /* ECC-756: Now decide whether this is legacy GRIB1 message. */
    /* Query data values to see if there is a mismatch */
    if (get_number_of_data_values(h, &numDataValues) == GRIB_SUCCESS) {
        if (*val != numDataValues) {
            if (h->context->debug)
                fprintf(stderr,
                        "ECCODES DEBUG number_of_points_gaussian: LEGACY MODE activated. "
                        "Count(=%ld) changed to num values(=%ld)\n",
                        *val, (long)numDataValues);
            *val = numDataValues;
        }
    }

    return err;
}
