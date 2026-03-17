/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_nearest_class_reduced.h"

eccodes::geo_nearest::Reduced _grib_nearest_reduced{};
eccodes::geo_nearest::Nearest* grib_nearest_reduced = &_grib_nearest_reduced;

namespace eccodes::geo_nearest {

#define NUM_NEIGHBOURS 4

int Reduced::init(grib_handle* h, grib_arguments* args)
{
    int ret = GRIB_SUCCESS;
    if ((ret = Gen::init(h, args) != GRIB_SUCCESS))
        return ret;

    Nj_      = args->get_name(h, cargs_++);
    pl_      = args->get_name(h, cargs_++);
    j_       = (size_t*)grib_context_malloc(h->context, 2 * sizeof(size_t));
    legacy_  = -1;
    rotated_ = -1;
    if (!j_)
        return GRIB_OUT_OF_MEMORY;
    k_ = (size_t*)grib_context_malloc(h->context, NUM_NEIGHBOURS * sizeof(size_t));
    if (!k_)
        return GRIB_OUT_OF_MEMORY;
    grib_get_long(h, "global", &global_);
    if (!global_) {
        int err;
        /*TODO longitudeOfFirstGridPointInDegrees from the def file*/
        if ((err = grib_get_double(h, "longitudeOfFirstGridPointInDegrees", &lon_first_)) != GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "grib_nearest_reduced: Unable to get longitudeOfFirstGridPointInDegrees %s\n",
                             grib_get_error_message(err));
            return err;
        }
        /*TODO longitudeOfLastGridPointInDegrees from the def file*/
        if ((err = grib_get_double(h, "longitudeOfLastGridPointInDegrees", &lon_last_)) != GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "grib_nearest_reduced: Unable to get longitudeOfLastGridPointInDegrees %s\n",
                             grib_get_error_message(err));
            return err;
        }
    }

    return ret;
}

typedef void (*get_reduced_row_proc)(long pl, double lon_first, double lon_last, long* npoints, long* ilon_first, long* ilon_last);

static int is_legacy(grib_handle* h, int* legacy)
{
    int err   = 0;
    long lVal = 0;
    *legacy   = 0;  // false by default
    err       = grib_get_long(h, "legacyGaussSubarea", &lVal);
    if (err) return err;
    *legacy = (int)lVal;
    return GRIB_SUCCESS;
}
static int is_rotated(grib_handle* h, int* rotated)
{
    int err   = 0;
    long lVal = 0;
    *rotated  = 0;  // false by default
    err       = grib_get_long(h, "isRotatedGrid", &lVal);
    if (err) return err;
    *rotated = (int)lVal;
    return GRIB_SUCCESS;
}

int Reduced::find(grib_handle* h,
                  double inlat, double inlon, unsigned long flags,
                  double* outlats, double* outlons, double* values,
                  double* distances, int* indexes, size_t* len)
{
    int err = 0;

    if (rotated_ == -1 || (flags & GRIB_NEAREST_SAME_GRID) == 0) {
        err = is_rotated(h, &(rotated_));
        if (err) return err;
    }

    if (global_ && rotated_ == 0) {
        err = find_global(h,
                          inlat, inlon, flags,
                          outlats, outlons, values,
                          distances, indexes, len);
    }
    else {
        /* ECC-762, ECC-1432: Use brute force generic algorithm
         * for reduced grid subareas. Review in the future
         */
        int lons_count = 0; /*dummy*/

        err = grib_nearest_find_generic(
            h, inlat, inlon, flags,
            values_key_,
            &(lats_),
            &(lats_count_),
            &(lons_),
            &(lons_count),
            &(distances_),
            outlats, outlons,
            values, distances, indexes, len);
    }
    return err;
}

/* Old implementation in src/deprecated/grib_nearest_class_reduced.old */
int Reduced::find_global(grib_handle* h,
                         double inlat, double inlon, unsigned long flags,
                         double* outlats, double* outlons, double* values,
                         double* distances, int* indexes, size_t* len)
{
    int err = 0, kk = 0, ii = 0;
    size_t jj           = 0;
    long* pla           = NULL;
    long* pl            = NULL;
    size_t nvalues      = 0;
    grib_iterator* iter = NULL;
    double lat = 0, lon = 0;
    double radiusInKm;
    int ilat = 0, ilon = 0;
    get_reduced_row_proc get_reduced_row_func = &grib_get_reduced_row;

    if (legacy_ == -1 || (flags & GRIB_NEAREST_SAME_GRID) == 0) {
        err = is_legacy(h, &(legacy_));
        if (err) return err;
    }
    if (legacy_ == 1) {
        get_reduced_row_func = &grib_get_reduced_row_legacy;
    }

    if ((err = grib_get_size(h, values_key_, &nvalues)) != GRIB_SUCCESS)
        return err;
    values_count_ = nvalues;

    if ((err = grib_nearest_get_radius(h, &radiusInKm)) != GRIB_SUCCESS)
        return err;

    /* Compute lat/lon info, create iterator etc if it's the 1st time or different grid.
     * This is for performance: if the grid has not changed, we only do this once
     * and reuse for other messages */
    if (!h_ || (flags & GRIB_NEAREST_SAME_GRID) == 0) {
        double olat = 1.e10;
        long n      = 0;

        ilat = 0;
        ilon = 0;
        if (grib_is_missing(h, Nj_, &err)) {
            grib_context_log(h->context, GRIB_LOG_DEBUG, "Key '%s' is missing", Nj_);
            return err ? err : GRIB_GEOCALCULUS_PROBLEM;
        }

        if ((err = grib_get_long(h, Nj_, &n)) != GRIB_SUCCESS)
            return err;
        lats_count_ = n;

        if (lats_)
            grib_context_free(h->context, lats_);
        lats_ = (double*)grib_context_malloc(h->context, lats_count_ * sizeof(double));
        if (!lats_)
            return GRIB_OUT_OF_MEMORY;

        if (lons_)
            grib_context_free(h->context, lons_);
        lons_ = (double*)grib_context_malloc(h->context, values_count_ * sizeof(double));
        if (!lons_)
            return GRIB_OUT_OF_MEMORY;

        iter = grib_iterator_new(h, GRIB_GEOITERATOR_NO_VALUES, &err);
        if (err != GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "grib_nearest_reduced: Unable to create lat/lon iterator");
            return err;
        }
        while (grib_iterator_next(iter, &lat, &lon, NULL)) {
            if (ilat < lats_count_ && olat != lat) {
                lats_[ilat++] = lat;
                olat          = lat;
            }
            while (lon > 360)
                lon -= 360;
            if (!global_) {       /* ECC-756 */
                if (legacy_ == 0) /*TODO*/
                    if (lon > 180 && lon < 360)
                        lon -= 360;
            }
            DEBUG_ASSERT_ACCESS(lons_, (long)ilon, (long)values_count_);
            lons_[ilon++] = lon;
        }
        lats_count_ = ilat;
        grib_iterator_delete(iter);
    }
    h_ = h;

    /* Compute distances if it's the 1st time or different point or different grid.
     * This is for performance: if the grid and the input point have not changed
     * we only do this once and reuse for other messages */
    if (!distances_ || (flags & GRIB_NEAREST_SAME_POINT) == 0 || (flags & GRIB_NEAREST_SAME_GRID) == 0) {
        double* lons           = NULL;
        int nlon               = 0;
        size_t plsize          = 0;
        long nplm1             = 0;
        int nearest_lons_found = 0;
        long row_count, ilon_first, ilon_last;

        if (global_) {
            inlon = normalise_longitude_in_degrees(inlon);
        }
        else {
            /* TODO: Experimental */
            if (legacy_ == 0)
                if (inlon > 180 && inlon < 360)
                    inlon -= 360;
        }

        ilat = lats_count_;
        if (lats_[ilat - 1] > lats_[0]) {
            if (inlat < lats_[0] || inlat > lats_[ilat - 1])
                return GRIB_OUT_OF_AREA;
        }
        else {
            if (inlat > lats_[0] || inlat < lats_[ilat - 1])
                return GRIB_OUT_OF_AREA;
        }

        if (!distances_)
            distances_ = (double*)grib_context_malloc(h->context, NUM_NEIGHBOURS * sizeof(double));
        if (!distances_)
            return GRIB_OUT_OF_MEMORY;

        grib_binary_search(lats_, ilat - 1, inlat, &(j_[0]), &(j_[1]));

        plsize = lats_count_;
        if ((err = grib_get_size(h, pl_, &plsize)) != GRIB_SUCCESS)
            return err;
        pla = (long*)grib_context_malloc(h->context, plsize * sizeof(long));
        if (!pla)
            return GRIB_OUT_OF_MEMORY;
        if ((err = grib_get_long_array(h, pl_, pla, &plsize)) != GRIB_SUCCESS)
            return err;

        pl = pla;
        while ((*pl) == 0) {
            pl++;
        }

        nlon = 0;
        if (global_) {
            for (jj = 0; jj < j_[0]; jj++)
                nlon += pl[jj];
            nplm1 = pl[j_[0]] - 1;
        }
        else {
            nlon = 0;
            for (jj = 0; jj < j_[0]; jj++) {
                row_count  = 0;
                ilon_first = 0;
                ilon_last  = 0;
                get_reduced_row_func(pl[jj], lon_first_, lon_last_, &row_count, &ilon_first, &ilon_last);
                nlon += row_count;
            }
            row_count  = 0;
            ilon_first = 0;
            ilon_last  = 0;
            get_reduced_row_func(pl[j_[0]], lon_first_, lon_last_, &row_count, &ilon_first, &ilon_last);
            nplm1 = row_count - 1;
        }
        lons = lons_ + nlon;

        nearest_lons_found = 0;
        /* ECC-756: The comparisons of longitudes here depends on the longitude values
         * from the point iterator. The old values could be -ve but the new algorithm
         * generates +ve values which break this test:
         *    lons[nplm1]>lons[0]
         */
        if (lons[nplm1] > lons[0]) {
            if (inlon < lons[0] || inlon > lons[nplm1]) {
                if (lons[nplm1] - lons[0] - 360 <= lons[nplm1] - lons[nplm1 - 1]) {
                    k_[0]              = 0;
                    k_[1]              = nplm1;
                    nearest_lons_found = 1;
                }
                else
                    return GRIB_OUT_OF_AREA;
            }
        }
        else {
            if (inlon > lons[0] || inlon < lons[nplm1]) {
                if (lons[0] - lons[nplm1] - 360 <= lons[0] - lons[1]) {
                    k_[0]              = 0;
                    k_[1]              = nplm1;
                    nearest_lons_found = 1;
                }
                else
                    return GRIB_OUT_OF_AREA;
            }
        }

        if (!nearest_lons_found) {
            if (!global_) {
                row_count  = 0;
                ilon_first = 0;
                ilon_last  = 0;
                get_reduced_row_func(pl[j_[0]], lon_first_, lon_last_, &row_count, &ilon_first, &ilon_last);
            }
            else {
                row_count = pl[j_[0]];
            }

            grib_binary_search(lons, row_count - 1, inlon,
                               &(k_[0]), &(k_[1]));
        }
        k_[0] += nlon;
        k_[1] += nlon;

        nlon = 0;
        if (global_) {
            for (jj = 0; jj < j_[1]; jj++)
                nlon += pl[jj];
            nplm1 = pl[j_[1]] - 1;
        }
        else {
            for (jj = 0; jj < j_[1]; jj++) {
                row_count  = 0;
                ilon_first = 0;
                ilon_last  = 0;
                get_reduced_row_func(pl[jj], lon_first_, lon_last_, &row_count, &ilon_first, &ilon_last);
                nlon += row_count;
            }
            row_count  = 0;
            ilon_first = 0;
            ilon_last  = 0;
            get_reduced_row_func(pl[j_[1]], lon_first_, lon_last_, &nplm1, &ilon_first, &ilon_last);
            nplm1--;
        }
        lons = lons_ + nlon;

        nearest_lons_found = 0;
        if (lons[nplm1] > lons[0]) {
            if (inlon < lons[0] || inlon > lons[nplm1]) {
                if (lons[nplm1] - lons[0] - 360 <=
                    lons[nplm1] - lons[nplm1 - 1]) {
                    k_[2]              = 0;
                    k_[3]              = nplm1;
                    nearest_lons_found = 1;
                }
                else
                    return GRIB_OUT_OF_AREA;
            }
        }
        else {
            if (inlon > lons[0] || inlon < lons[nplm1]) {
                if (lons[0] - lons[nplm1] - 360 <=
                    lons[0] - lons[1]) {
                    k_[2]              = 0;
                    k_[3]              = nplm1;
                    nearest_lons_found = 1;
                }
                else
                    return GRIB_OUT_OF_AREA;
            }
        }

        if (!nearest_lons_found) {
            if (!global_) {
                row_count  = 0;
                ilon_first = 0;
                ilon_last  = 0;
                get_reduced_row_func(pl[j_[1]], lon_first_, lon_last_, &row_count, &ilon_first, &ilon_last);
            }
            else {
                row_count = pl[j_[1]];
            }

            grib_binary_search(lons, row_count - 1, inlon,
                               &(k_[2]), &(k_[3]));
        }

        k_[2] += nlon;
        k_[3] += nlon;

        kk = 0;
        for (jj = 0; jj < 2; jj++) {
            for (ii = 0; ii < 2; ii++) {
                distances_[kk] = geographic_distance_spherical(radiusInKm, inlon, inlat,
                                                               lons_[k_[kk]], lats_[j_[jj]]);
                kk++;
            }
        }

        grib_context_free(h->context, pla);
    }

    kk = 0;
    if (values) {
        /* See ECC-1403 and ECC-499 */
        /* Performance: Decode the field once and get all 4 values */
        err = grib_get_double_element_set(h, values_key_, k_, NUM_NEIGHBOURS, values);
        if (err != GRIB_SUCCESS) return err;
    }
    for (jj = 0; jj < 2; jj++) {
        for (ii = 0; ii < 2; ii++) {
            distances[kk] = distances_[kk];
            outlats[kk]   = lats_[j_[jj]];
            outlons[kk]   = lons_[k_[kk]];
            /*if (values) {
             *    grib_get_double_element_internal(h, values_key_, k_[kk], &(values[kk]));
             *}
             */
            if (k_[kk] >= INT_MAX) {
                /* Current interface uses an 'int' for 'indexes' which is 32bits! We should change this to a 64bit type */
                grib_context_log(h->context, GRIB_LOG_ERROR, "grib_nearest_reduced: Unable to compute index. Value too large");
                return GRIB_OUT_OF_RANGE;
            }
            else {
                indexes[kk] = (int)k_[kk];
            }
            kk++;
        }
    }

    return GRIB_SUCCESS;
}

}  // namespace eccodes::geo_nearest
