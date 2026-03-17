/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_nearest_class_regular.h"

eccodes::geo_nearest::Regular _grib_nearest_regular{};
eccodes::geo_nearest::Nearest* grib_nearest_regular = &_grib_nearest_regular;

namespace eccodes::geo_nearest {

#define NUM_NEIGHBOURS 4

int Regular::init(grib_handle* h, grib_arguments* args)
{
    int ret = GRIB_SUCCESS;
    if ((ret = Gen::init(h, args) != GRIB_SUCCESS))
        return ret;

    Ni_ = args->get_name(h, cargs_++);
    Nj_ = args->get_name(h, cargs_++);
    i_  = (size_t*)grib_context_malloc(h->context, 2 * sizeof(size_t));
    j_  = (size_t*)grib_context_malloc(h->context, 2 * sizeof(size_t));
    return ret;
}

static bool is_rotated_grid(grib_handle* h)
{
    long is_rotated = 0;
    int err         = grib_get_long(h, "isRotatedGrid", &is_rotated);
    if (!err && is_rotated)
        return true;
    return false;
}

// Old implementation in
//  src/deprecated/grib_nearest_class_regular.cc
//
int Regular::find(grib_handle* h,
                double inlat, double inlon, unsigned long flags,
                double* outlats, double* outlons,
                double* values, double* distances, int* indexes, size_t* len)
{
    int ret = 0, kk = 0, ii = 0, jj = 0;
    size_t nvalues = 0;
    double radiusInKm;

    grib_iterator* iter = NULL;
    double lat = 0, lon = 0;
    const bool is_rotated  = is_rotated_grid(h);
    double angleOfRotation = 0, southPoleLat = 0, southPoleLon = 0;
    grib_context* c = h->context;

    while (inlon < 0)
        inlon += 360;
    while (inlon > 360)
        inlon -= 360;

    if ((ret = grib_get_size(h, values_key_, &nvalues)) != GRIB_SUCCESS)
        return ret;
    values_count_ = nvalues;

    if ((ret = grib_nearest_get_radius(h, &radiusInKm)) != GRIB_SUCCESS)
        return ret;

    /* Compute lat/lon info, create iterator etc if it's the 1st time or different grid.
     * This is for performance: if the grid has not changed, we only do this once
     * and reuse for other messages */
    if (!h_ || (flags & GRIB_NEAREST_SAME_GRID) == 0) {
        double olat = 1.e10, olon = 1.e10;
        int ilat = 0, ilon = 0;
        long n = 0;

        if (grib_is_missing(h, Ni_, &ret)) {
            grib_context_log(h->context, GRIB_LOG_DEBUG, "Key '%s' is missing", Ni_);
            return ret ? ret : GRIB_GEOCALCULUS_PROBLEM;
        }

        if (grib_is_missing(h, Nj_, &ret)) {
            grib_context_log(h->context, GRIB_LOG_DEBUG, "Key '%s' is missing", Nj_);
            return ret ? ret : GRIB_GEOCALCULUS_PROBLEM;
        }

        /* ECC-600: Support for rotated grids
         * First:   rotate the input point
         * Then:    run the lat/lon iterator over the rotated grid (disableUnrotate)
         * Finally: unrotate the resulting point
         */
        if (is_rotated) {
            double new_lat = 0, new_lon = 0;
            ret = grib_get_double_internal(h, "angleOfRotation", &angleOfRotation);
            if (ret)
                return ret;
            ret = grib_get_double_internal(h, "latitudeOfSouthernPoleInDegrees", &southPoleLat);
            if (ret)
                return ret;
            ret = grib_get_double_internal(h, "longitudeOfSouthernPoleInDegrees", &southPoleLon);
            if (ret)
                return ret;
            ret = grib_set_long(h, "iteratorDisableUnrotate", 1);
            if (ret)
                return ret;
            /* Rotate the inlat, inlon */
            rotate(inlat, inlon, angleOfRotation, southPoleLat, southPoleLon, &new_lat, &new_lon);
            inlat = new_lat;
            inlon = new_lon;
            /*if(h->context->debug) printf("nearest find: rotated grid: new point=(%g,%g)\n",new_lat,new_lon);*/
        }

        if ((ret = grib_get_long(h, Ni_, &n)) != GRIB_SUCCESS)
            return ret;
        lons_count_ = n;

        if ((ret = grib_get_long(h, Nj_, &n)) != GRIB_SUCCESS)
            return ret;
        lats_count_ = n;

        if (lats_)
            grib_context_free(c, lats_);
        lats_ = (double*)grib_context_malloc(c, lats_count_ * sizeof(double));
        if (!lats_)
            return GRIB_OUT_OF_MEMORY;

        if (lons_)
            grib_context_free(c, lons_);
        lons_ = (double*)grib_context_malloc(c, lons_count_ * sizeof(double));
        if (!lons_)
            return GRIB_OUT_OF_MEMORY;

        iter = grib_iterator_new(h, GRIB_GEOITERATOR_NO_VALUES, &ret);
        if (ret != GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "grib_nearest_regular: Unable to create lat/lon iterator");
            return ret;
        }
        while (grib_iterator_next(iter, &lat, &lon, NULL)) {
            if (ilat < lats_count_ && olat != lat) {
                lats_[ilat++] = lat;
                olat               = lat;
            }
            if (ilon < lons_count_ && olon != lon) {
                lons_[ilon++] = lon;
                olon               = lon;
            }
        }
        grib_iterator_delete(iter);
    }
    h_ = h;

    /* Compute distances if it's the 1st time or different point or different grid.
     * This is for performance: if the grid and the input point have not changed
     * we only do this once and reuse for other messages */
    if (!distances_ || (flags & GRIB_NEAREST_SAME_POINT) == 0 || (flags & GRIB_NEAREST_SAME_GRID) == 0) {
        int nearest_lons_found = 0;

        if (lats_[lats_count_ - 1] > lats_[0]) {
            if (inlat < lats_[0] || inlat > lats_[lats_count_ - 1])
                return GRIB_OUT_OF_AREA;
        }
        else {
            if (inlat > lats_[0] || inlat < lats_[lats_count_ - 1])
                return GRIB_OUT_OF_AREA;
        }

        if (lons_[lons_count_ - 1] > lons_[0]) {
            if (inlon < lons_[0] || inlon > lons_[lons_count_ - 1]) {
                /* try to scale*/
                if (inlon > 0)
                    inlon -= 360;
                else
                    inlon += 360;

                if (inlon < lons_[0] || inlon > lons_[lons_count_ - 1]) {
                    if (lons_[0] + 360 - lons_[lons_count_ - 1] <=
                        lons_[1] - lons_[0]) {
                        /*it's a global field in longitude*/
                        i_[0]         = 0;
                        i_[1]         = lons_count_ - 1;
                        nearest_lons_found = 1;
                    }
                    else
                        return GRIB_OUT_OF_AREA;
                }
            }
        }
        else {
            if (inlon > lons_[0] || inlon < lons_[lons_count_ - 1]) {
                /* try to scale*/
                if (inlon > 0)
                    inlon -= 360;
                else
                    inlon += 360;
                if (lons_[0] - lons_[lons_count_ - 1] - 360 <=
                    lons_[0] - lons_[1]) {
                    /*it's a global field in longitude*/
                    i_[0]         = 0;
                    i_[1]         = lons_count_ - 1;
                    nearest_lons_found = 1;
                }
                else if (inlon > lons_[0] || inlon < lons_[lons_count_ - 1])
                    return GRIB_OUT_OF_AREA;
            }
        }

        grib_binary_search(lats_, lats_count_ - 1, inlat,
                           &(j_[0]), &(j_[1]));

        if (!nearest_lons_found)
            grib_binary_search(lons_, lons_count_ - 1, inlon,
                               &(i_[0]), &(i_[1]));

        if (!distances_)
            distances_ = (double*)grib_context_malloc(c, NUM_NEIGHBOURS * sizeof(double));
        if (!k_)
            k_ = (size_t*)grib_context_malloc(c, NUM_NEIGHBOURS * sizeof(size_t));
        kk = 0;
        for (jj = 0; jj < 2; jj++) {
            for (ii = 0; ii < 2; ii++) {
                k_[kk]         = i_[ii] + lons_count_ * j_[jj];
                distances_[kk] = geographic_distance_spherical(radiusInKm, inlon, inlat,
                                                            lons_[i_[ii]], lats_[j_[jj]]);
                kk++;
            }
        }
    }

    kk = 0;

    /*
     * Brute force algorithm:
     * First unpack all the values into an array. Then when we need the 4 points
     * we just index into this array so no need to call grib_get_double_element_internal
     *
     *   if (nearest->values) grib_context_free(c,nearest->values);
     *   nearest->values = grib_context_malloc(h->context,nvalues*sizeof(double));
     *   if (!nearest->values) return GRIB_OUT_OF_MEMORY;
     *   ret = grib_get_double_array(h, values_key_, nearest->values ,&nvalues);
     *   if (ret) return ret;
     */

    if (values) {
        /* See ECC-1403 and ECC-499 */
        /* Performance: Decode the field once and get all 4 values */
        if ((ret = grib_get_double_element_set(h, values_key_, k_, NUM_NEIGHBOURS, values)) != GRIB_SUCCESS)
            return ret;
    }

    for (jj = 0; jj < 2; jj++) {
        for (ii = 0; ii < 2; ii++) {
            distances[kk] = distances_[kk];
            outlats[kk]   = lats_[j_[jj]];
            outlons[kk]   = lons_[i_[ii]];
            if (is_rotated) {
                /* Unrotate resulting lat/lon */
                double new_lat = 0, new_lon = 0;
                unrotate(outlats[kk], outlons[kk], angleOfRotation, southPoleLat, southPoleLon, &new_lat, &new_lon);
                outlats[kk] = new_lat;
                outlons[kk] = new_lon;
            }
            /* See ECC-1403 and ECC-499
             * if (values) {
             *   grib_get_double_element_internal(h, values_key_, k_[kk], &(values[kk]));
             *}
             */
            /* Using the brute force approach described above */
            /* ECCODES_ASSERT(k_[kk] < nvalues); */
            /* values[kk]=nearest->values[k_[kk]]; */

            if (k_[kk] >= INT_MAX) {
                /* Current interface uses an 'int' for 'indexes' which is 32bits! We should change this to a 64bit type */
                grib_context_log(h->context, GRIB_LOG_ERROR, "grib_nearest_regular: Unable to compute index. Value too large");
                return GRIB_OUT_OF_RANGE;
            } else {
                indexes[kk] = (int)k_[kk];
            }
            kk++;
        }
    }

    return GRIB_SUCCESS;
}

}  // namespace eccodes::geo_nearest
