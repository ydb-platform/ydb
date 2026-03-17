/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_nearest_class_latlon_reduced.h"

eccodes::geo_nearest::LatlonReduced _grib_nearest_latlon_reduced{};
eccodes::geo_nearest::Nearest* grib_nearest_latlon_reduced = &_grib_nearest_latlon_reduced;

namespace eccodes::geo_nearest {

int LatlonReduced::init(grib_handle* h, grib_arguments* args)
{
    int ret = GRIB_SUCCESS;
    if ((ret = Gen::init(h, args) != GRIB_SUCCESS))
        return ret;

    Nj_       = args->get_name(h, cargs_++);
    pl_       = args->get_name(h, cargs_++);
    lonFirst_ = args->get_name(h, cargs_++);
    lonLast_  = args->get_name(h, cargs_++);
    j_        = (size_t*)grib_context_malloc(h->context, 2 * sizeof(size_t));
    if (!j_)
        return GRIB_OUT_OF_MEMORY;
    k_ = (size_t*)grib_context_malloc(h->context, 4 * sizeof(size_t));
    if (!k_)
        return GRIB_OUT_OF_MEMORY;

    return ret;
}

int LatlonReduced::find(grib_handle* h,
                double inlat, double inlon, unsigned long flags,
                double* outlats, double* outlons, double* values,
                double* distances, int* indexes, size_t* len)
{
    int err = 0;
    double lat1, lat2, lon1, lon2;
    int is_global = 1;

    if (grib_get_double(h, "longitudeFirstInDegrees", &lon1) == GRIB_SUCCESS &&
        grib_get_double(h, "longitudeLastInDegrees", &lon2) == GRIB_SUCCESS &&
        grib_get_double(h, "latitudeFirstInDegrees", &lat1) == GRIB_SUCCESS &&
        grib_get_double(h, "latitudeLastInDegrees", &lat2) == GRIB_SUCCESS)
    {
        const double difflat = fabs(lat1-lat2);
        if (difflat < 180 || lon1 != 0 || lon2 < 359) {
            is_global = 0; /* subarea */
        }
    }

    if (is_global) {
        err = find_global(h, inlat, inlon, flags,
                outlats, outlons, values,
                distances, indexes, len);
    }
    else
    {
        int lons_count = 0;  /*dummy*/
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

int LatlonReduced::find_global(grib_handle* h,
                double inlat, double inlon, unsigned long flags,
                double* outlats, double* outlons, double* values,
                double* distances, int* indexes, size_t* len)
{
    int ret = 0, kk = 0, ii = 0, jj = 0;
    int j               = 0;
    long* pla           = NULL;
    long* pl            = NULL;
    size_t nvalues      = 0;
    grib_iterator* iter = NULL;
    double lat = 0, lon = 0;
    double radiusInKm;
    int ilat = 0, ilon = 0;

    if ((ret = grib_get_size(h, values_key_, &nvalues)) != GRIB_SUCCESS)
        return ret;
    values_count_ = nvalues;

    if ((ret = grib_nearest_get_radius(h, &radiusInKm)) != GRIB_SUCCESS)
        return ret;

    /* Compute lat/lon info, create iterator etc if it's the 1st time or different grid.
     * This is for performance: if the grid has not changed, we only do this once
     * and reuse for other messages */
    if (!h_ || (flags & GRIB_NEAREST_SAME_GRID) == 0) {
        double olat  = 1.e10;
        long n       = 0;

        ilat = 0;
        ilon = 0;
        if (grib_is_missing(h, Nj_, &ret)) {
            grib_context_log(h->context, GRIB_LOG_DEBUG, "Key '%s' is missing", Nj_);
            return ret ? ret : GRIB_GEOCALCULUS_PROBLEM;
        }

        if ((ret = grib_get_long(h, Nj_, &n)) != GRIB_SUCCESS)
            return ret;
        lats_count_ = n;

        if (lats_)
            grib_context_free(h->context, lats_);
        lats_ = (double*)grib_context_malloc(h->context,
                                                  lats_count_ * sizeof(double));
        if (!lats_)
            return GRIB_OUT_OF_MEMORY;

        if (lons_)
            grib_context_free(h->context, lons_);
        lons_ = (double*)grib_context_malloc(h->context,
                                                  values_count_ * sizeof(double));
        if (!lons_)
            return GRIB_OUT_OF_MEMORY;

        iter = grib_iterator_new(h, GRIB_GEOITERATOR_NO_VALUES, &ret);
        if (ret) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to create iterator");
            return ret;
        }
        while (grib_iterator_next(iter, &lat, &lon, NULL)) {
            if (ilat < lats_count_ && olat != lat) {
                lats_[ilat++] = lat;
                olat               = lat;
            }
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
        double lon_first, lon_last;
        int islocal = 0;
        long plmax;
        double dimin;

        if ((ret = grib_get_double(h, lonFirst_, &lon_first)) != GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "grib_nearest_latlon_reduced.find(): unable to get %s %s\n", lonFirst_,
                             grib_get_error_message(ret));
            return ret;
        }
        if ((ret = grib_get_double(h, lonLast_, &lon_last)) != GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "grib_nearest_latlon_reduced.find(): unable to get %s %s\n", lonLast_,
                             grib_get_error_message(ret));
            return ret;
        }

        plsize = lats_count_;
        if ((ret = grib_get_size(h, pl_, &plsize)) != GRIB_SUCCESS)
            return ret;
        pla = (long*)grib_context_malloc(h->context, plsize * sizeof(long));
        if (!pla)
            return GRIB_OUT_OF_MEMORY;
        if ((ret = grib_get_long_array(h, pl_, pla, &plsize)) != GRIB_SUCCESS)
            return ret;

        pl = pla;
        while ((*pl) == 0) {
            pl++;
        }

        plmax = pla[0];
        for (j = 0; j < plsize; j++)
            if (plmax < pla[j])
                plmax = pla[j];
        dimin = 360.0 / plmax;

        if (360 - fabs(lon_last - lon_first) < 2 * dimin) {
            islocal = 0;
        }
        else {
            islocal = 1;
        }

        if (islocal)
            for (j = 0; j < plsize; j++)
                pla[j]--;

        /* printf("XXXX islocal=%d\n",islocal); */
        while (inlon < 0)
            inlon += 360;
        while (inlon > 360)
            inlon -= 360;

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
            distances_ = (double*)grib_context_malloc(h->context, 4 * sizeof(double));
        if (!distances_)
            return GRIB_OUT_OF_MEMORY;

        grib_binary_search(lats_, ilat - 1, inlat, &(j_[0]), &(j_[1]));

        nlon = 0;
        for (jj = 0; jj < j_[0]; jj++)
            nlon += pl[jj];
        nplm1 = pl[j_[0]] - 1;

        lons = lons_ + nlon;

        nearest_lons_found = 0;
        if (lons[nplm1] > lons[0]) {
            if (inlon < lons[0] || inlon > lons[nplm1]) {
                if (lons[nplm1] - lons[0] - 360 <=
                    lons[nplm1] - lons[nplm1 - 1]) {
                    k_[0]         = 0;
                    k_[1]         = nplm1;
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
                    k_[0]         = 0;
                    k_[1]         = nplm1;
                    nearest_lons_found = 1;
                }
                else
                    return GRIB_OUT_OF_AREA;
            }
        }

        if (!nearest_lons_found) {
            grib_binary_search(lons, pl[j_[0]] - 1, inlon,
                               &(k_[0]), &(k_[1]));
        }
        k_[0] += nlon;
        k_[1] += nlon;

        nlon = 0;
        for (jj = 0; jj < j_[1]; jj++)
            nlon += pl[jj];
        nplm1 = pl[j_[1]] - 1;

        lons = lons_ + nlon;

        nearest_lons_found = 0;
        if (lons[nplm1] > lons[0]) {
            if (inlon < lons[0] || inlon > lons[nplm1]) {
                if (lons[nplm1] - lons[0] - 360 <=
                    lons[nplm1] - lons[nplm1 - 1]) {
                    k_[2]         = 0;
                    k_[3]         = nplm1;
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
                    k_[2]         = 0;
                    k_[3]         = nplm1;
                    nearest_lons_found = 1;
                }
                else
                    return GRIB_OUT_OF_AREA;
            }
        }

        if (!nearest_lons_found) {
            grib_binary_search(lons, pl[j_[1]] - 1, inlon,
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
    for (jj = 0; jj < 2; jj++) {
        for (ii = 0; ii < 2; ii++) {
            distances[kk] = distances_[kk];
            outlats[kk]   = lats_[j_[jj]];
            outlons[kk]   = lons_[k_[kk]];
            if (values) { /* ECC-499 */
                grib_get_double_element_internal(h, values_key_, k_[kk], &(values[kk]));
            }
            indexes[kk] = (int)k_[kk];
            kk++;
        }
    }

    return GRIB_SUCCESS;
}

}  // namespace eccodes::geo_nearest
