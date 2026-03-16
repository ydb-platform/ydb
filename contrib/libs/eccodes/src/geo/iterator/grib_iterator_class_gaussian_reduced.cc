/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_gaussian_reduced.h"
#include <cmath>

eccodes::geo_iterator::GaussianReduced _grib_iterator_gaussian_reduced{};
eccodes::geo_iterator::Iterator* grib_iterator_gaussian_reduced = &_grib_iterator_gaussian_reduced;

namespace eccodes::geo_iterator {

#define ITER "Reduced Gaussian grid Geoiterator"

int GaussianReduced::next(double* lat, double* lon, double* val) const
{
    double ret_lat = 0, ret_lon = 0;

    if (e_ >= (long)(nv_ - 1))
        return 0;
    e_++;

    ret_lat = lats_[e_];
    ret_lon = lons_[e_];
    if (val && data_) {
        *val = data_[e_];
    }

    if (isRotated_ && !disableUnrotate_) {
        double new_lat = 0, new_lon = 0;
        unrotate(ret_lat, ret_lon,
                 angleOfRotation_, southPoleLat_, southPoleLon_,
                 &new_lat, &new_lon);
        ret_lat = new_lat;
        ret_lon = new_lon;
    }
    *lat = ret_lat;
    *lon = ret_lon;

    return 1;
}

typedef void (*get_reduced_row_proc)(long pl, double lon_first, double lon_last, long* npoints, long* ilon_first, long* ilon_last);

/* For a reduced Gaussian grid which is GLOBAL, the number of points is the sum of the 'pl' array */
/* i.e. the total number of points on all latitudes */
static size_t count_global_points(const long* pl, size_t plsize)
{
    return sum_of_pl_array(pl, plsize);
}
static size_t count_subarea_points(grib_handle* h, get_reduced_row_proc get_reduced_row,
                                   long* pl, size_t plsize, double lon_first, double lon_last)
{
    size_t j = 0, result = 0;
    long row_count  = 0;
    long ilon_first = 0, ilon_last = 0; /*unused*/
    long Nj = 0;
    grib_get_long_internal(h, "Nj", &Nj);
    for (j = 0; j < Nj; j++) {
        row_count = 0;
        get_reduced_row(pl[j], lon_first, lon_last, &row_count, &ilon_first, &ilon_last);
        result += row_count;
    }
    return result;
}

/* Search for 'x' in the array 'xx' (the index of last element being 'n') and return index in 'j' */
static void binary_search(const double xx[], const unsigned long n, double x, long* j)
{
    /*This routine works only on descending ordered arrays*/
#define EPSILON 1e-3

    unsigned long ju, jm, jl;
    jl = 0;
    ju = n;
    while (ju - jl > 1) {
        jm = (ju + jl) >> 1;
        if (fabs(x - xx[jm]) < EPSILON) {
            /* found something close enough. We're done */
            *j = jm;
            return;
        }
        if (x < xx[jm])
            jl = jm;
        else
            ju = jm;
    }
    *j = jl;
}

/* Use legacy way to compute the iterator latitude/longitude values */
int GaussianReduced::iterate_reduced_gaussian_subarea_legacy(grib_handle* h,
                                                             double lat_first, double lon_first,
                                                             double lat_last, double lon_last,
                                                             double* lats, long* pl, size_t plsize)
{
    int err        = 0;
    int l          = 0;
    size_t j       = 0;
    long row_count = 0;
    double d       = 0;
    long ilon_first, ilon_last, i;
    /*get_reduced_row_proc get_reduced_row = &grib_get_reduced_row;*/
    get_reduced_row_proc get_reduced_row = &grib_get_reduced_row_legacy; /* legacy algorithm */

    if (h->context->debug) {
        const size_t np = count_subarea_points(h, get_reduced_row, pl, plsize, lon_first, lon_last);
        fprintf(stderr, "ECCODES DEBUG grib_iterator_class_gaussian_reduced: Legacy sub-area num points=%zu\n", np);
    }

    /*find starting latitude */
    d = fabs(lats[0] - lats[1]);
    while (fabs(lat_first - lats[l]) > d) {
        l++;
    }

    e_ = 0;
    for (j = 0; j < plsize; j++) {
        long k    = 0;
        row_count = 0;
        get_reduced_row(pl[j], lon_first, lon_last, &row_count, &ilon_first, &ilon_last);
        /*printf("iterate_reduced_gaussian_subarea %ld %g %g count=%ld, (i1=%ld, i2=%ld)\n",pl[j],lon_first,lon_last, row_count, ilon_first,ilon_last);*/
        if (ilon_first > ilon_last)
            ilon_first -= pl[j];
        for (i = ilon_first; i <= ilon_last; i++) {
            if (e_ >= nv_) {
                size_t np = count_subarea_points(h, get_reduced_row, pl, plsize, lon_first, lon_last);
                grib_context_log(h->context, GRIB_LOG_ERROR,
                                 "%s (sub-area legacy). Num points=%zu, size(values)=%zu", ITER, np, nv_);
                return GRIB_WRONG_GRID;
            }

            lons_[e_] = ((i) * 360.0) / pl[j];
            lats_[e_] = lats[j + l];
            e_++;
            k++;
            if (k >= row_count) {
                /* Ensure we exit the loop and only process 'row_count' points */
                break;
            }
        }
    }
    return err;
}

// ECC-747
// Try legacy approach, if that fails try the next algorithm
//     int err = iterate_reduced_gaussian_subarea(iter, h, lat_first, lon_first, lat_last, lon_last, lats, pl, plsize, 0);
//     if (err == GRIB_WRONG_GRID) {
//         /* ECC-445: First attempt failed. Try again with a different algorithm */
//         err = iterate_reduced_gaussian_subarea_algorithm2(iter, h, lat_first, lon_first, lat_last, lon_last, lats, pl, plsize);
//     }
//     return err;
int GaussianReduced::iterate_reduced_gaussian_subarea(grib_handle* h,
                                                      double lat_first, double lon_first,
                                                      double lat_last, double lon_last,
                                                      double* lats, long* pl, size_t plsize, size_t numlats)
{
    int err        = 0;
    long l         = 0;
    size_t j       = 0;
    long row_count = 0, i = 0;
    double olon_first, olon_last;
    get_reduced_row_proc get_reduced_row = &grib_get_reduced_row;

    if (h->context->debug) {
        const size_t np = count_subarea_points(h, get_reduced_row, pl, plsize, lon_first, lon_last);
        fprintf(stderr, "ECCODES DEBUG grib_iterator_class_gaussian_reduced: sub-area num points=%zu\n", np);
    }

    /* Find starting latitude */
    binary_search(lats, numlats - 1, lat_first, &l);
    ECCODES_ASSERT(l < numlats);

    //     for(il=0; il<numlats; ++il) {
    //         const double diff = fabs(lat_first-lats[il]);
    //         if (diff < min_d) {
    //             min_d = diff;
    //             l = il; /* index of the latitude */
    //         }
    //     }

    e_ = 0;
    for (j = 0; j < plsize; j++) {
        const double delta = 360.0 / pl[j];
        row_count          = 0;
        grib_get_reduced_row_p(pl[j], lon_first, lon_last, &row_count, &olon_first, &olon_last);
        for (i = 0; i < row_count; ++i) {
            double lon2 = olon_first + i * delta;
            if (e_ >= nv_) {
                /* Only print error message on the second pass */
                size_t np = count_subarea_points(h, get_reduced_row, pl, plsize, lon_first, lon_last);
                grib_context_log(h->context, GRIB_LOG_ERROR,
                                 "%s (sub-area). Num points=%zu, size(values)=%zu", ITER, np, nv_);
                return GRIB_WRONG_GRID;
            }
            lons_[e_] = lon2;
            DEBUG_ASSERT(j + l < numlats);
            lats_[e_] = lats[j + l];
            e_++;
        }
    }

    if (e_ != nv_) {
        /* Fewer counted points in the sub-area than the number of data values */
        const size_t legacy_count = count_subarea_points(h, grib_get_reduced_row_legacy, pl, plsize, lon_first, lon_last);
        if (nv_ == legacy_count) {
            /* Legacy (produced by PRODGEN/LIBEMOS) */
            return iterate_reduced_gaussian_subarea_legacy(h, lat_first, lon_first, lat_last, lon_last, lats, pl, plsize);
        }
        else {
            /* TODO: A gap exists! Not all values can be mapped. Inconsistent grid or error in calculating num. points! */
        }
    }
    return err;
}

int GaussianReduced::init(grib_handle* h, grib_arguments* args)
{
    int ret = GRIB_SUCCESS;
    if ((ret = Gen::init(h, args)) != GRIB_SUCCESS)
        return ret;

    int j, is_global = 0;
    double lat_first = 0, lon_first = 0, lat_last = 0, lon_last = 0;
    double angular_precision = 1.0 / 1000000.0;
    double* lats;
    size_t plsize  = 0;
    size_t numlats = 0;
    long* pl;
    long max_pl = 0;
    long nj = 0, order = 0, i;
    long row_count         = 0;
    long angleSubdivisions = 0;
    const grib_context* c  = h->context;
    const char* slat_first = args->get_name(h, carg_++);
    const char* slon_first = args->get_name(h, carg_++);
    const char* slat_last  = args->get_name(h, carg_++);
    const char* slon_last  = args->get_name(h, carg_++);
    const char* sorder     = args->get_name(h, carg_++);
    const char* spl        = args->get_name(h, carg_++);
    const char* snj        = args->get_name(h, carg_++);

    angleOfRotation_ = 0;
    isRotated_       = 0;
    southPoleLat_    = 0;
    southPoleLon_    = 0;
    disableUnrotate_ = 0; /* unrotate enabled by default */

    ret = grib_get_long(h, "isRotatedGrid", &isRotated_);
    if (ret == GRIB_SUCCESS && isRotated_) {
        if ((ret = grib_get_double_internal(h, "angleOfRotation", &angleOfRotation_)))
            return ret;
        if ((ret = grib_get_double_internal(h, "latitudeOfSouthernPoleInDegrees", &southPoleLat_)))
            return ret;
        if ((ret = grib_get_double_internal(h, "longitudeOfSouthernPoleInDegrees", &southPoleLon_)))
            return ret;
    }

    if ((ret = grib_get_double_internal(h, slat_first, &lat_first)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, slon_first, &lon_first)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, slat_last, &lat_last)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, slon_last, &lon_last)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(h, sorder, &order)) != GRIB_SUCCESS)
        return ret;
    if (order == 0) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Invalid grid: N cannot be 0!", ITER);
        return GRIB_WRONG_GRID;
    }
    if ((ret = grib_get_long_internal(h, snj, &nj)) != GRIB_SUCCESS)
        return ret;

    if (grib_get_long(h, "angleSubdivisions", &angleSubdivisions) == GRIB_SUCCESS) {
        ECCODES_ASSERT(angleSubdivisions > 0);
        angular_precision = 1.0 / angleSubdivisions;
    }

    numlats = order * 2;
    lats    = (double*)grib_context_malloc(h->context, sizeof(double) * numlats);
    if (!lats)
        return GRIB_OUT_OF_MEMORY;
    if ((ret = grib_get_gaussian_latitudes(order, lats)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_size(h, spl, &plsize)) != GRIB_SUCCESS)
        return ret;

    ECCODES_ASSERT(plsize);
    pl = (long*)grib_context_malloc(c, sizeof(long) * plsize);
    if (!pl)
        return GRIB_OUT_OF_MEMORY;

    grib_get_long_array_internal(h, spl, pl, &plsize);

    lats_ = (double*)grib_context_malloc_clear(h->context, nv_ * sizeof(double));
    if (!lats_)
        return GRIB_OUT_OF_MEMORY;
    lons_ = (double*)grib_context_malloc_clear(h->context, nv_ * sizeof(double));
    if (!lons_)
        return GRIB_OUT_OF_MEMORY;

    while (lon_last < 0)
        lon_last += 360;
    while (lon_first < 0)
        lon_first += 360;

    /* Find the maximum element of "pl" array, do not assume it's 4*N! */
    /* This could be an Octahedral Gaussian Grid */
    max_pl = pl[0];
    for (j = 1; j < plsize; j++) {
        if (pl[j] > max_pl)
            max_pl = pl[j];
    }

    is_global = is_gaussian_global(lat_first, lat_last, lon_first, lon_last, max_pl, lats, angular_precision);
    if (!is_global) {
        /*sub area*/
        ret = iterate_reduced_gaussian_subarea(h, lat_first, lon_first, lat_last, lon_last, lats, pl, plsize, numlats);
    }
    else {
        /*global*/
        e_ = 0;
        if (h->context->debug) {
            const size_t np = count_global_points(pl, plsize);
            fprintf(stderr, "ECCODES DEBUG grib_iterator_class_gaussian_reduced: global num points=%zu\n", np);
        }

        for (j = 0; j < plsize; j++) {
            row_count = pl[j];
            for (i = 0; i < row_count; i++) {
                if (e_ >= nv_) {
                    /*grib_context_log(h->context,GRIB_LOG_ERROR, "Failed to initialise reduced Gaussian iterator (global)");*/
                    /*return GRIB_WRONG_GRID;*/
                    /*Try now as NON-global*/
                    ret = iterate_reduced_gaussian_subarea(h, lat_first, lon_first, lat_last, lon_last, lats, pl, plsize, numlats);
                    if (ret != GRIB_SUCCESS)
                        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Failed to initialise iterator (global)", ITER);
                    goto finalise;
                }

                lons_[e_] = (i * 360.0) / row_count;
                lats_[e_] = lats[j];
                e_++;
            }
        }
    }

finalise:
    e_ = -1;
    grib_context_free(h->context, lats);
    grib_context_free(h->context, pl);

    return ret;
}

int GaussianReduced::destroy()
{
    DEBUG_ASSERT(h_);
    const grib_context* c = h_->context;
    grib_context_free(c, lats_);
    grib_context_free(c, lons_);

    return Gen::destroy();
}

} // namespace eccodes::geo_iterator
