/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_longitudes.h"

grib_accessor_longitudes_t _grib_accessor_longitudes{};
grib_accessor* grib_accessor_longitudes = &_grib_accessor_longitudes;

static int get_distinct(grib_accessor* a, double** val, long* len);
void grib_accessor_longitudes_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n = 0;

    values_   = c->get_name(grib_handle_of_accessor(this), n++);
    distinct_ = c->get_long(grib_handle_of_accessor(this), n++);
    save_     = 0;
    lons_     = 0;

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}


int grib_accessor_longitudes_t::unpack_double(double* val, size_t* len)
{
    grib_context* c     = context_;
    int ret             = 0;
    double* v           = val;
    double dummyLat     = 0;
    size_t size         = 0;
    long count          = 0;
    grib_iterator* iter = NULL;

    save_ = 1;
    ret   = value_count(&count);
    if (ret) return ret;
    size = count;

    if (*len < size) {
        /* lons_ are computed in value_count*/
        if (lons_) {
            grib_context_free(c, lons_);
            lons_ = NULL;
        }
        return GRIB_ARRAY_TOO_SMALL;
    }
    save_ = 0;

    /* lons_ are computed in value_count*/
    if (lons_) {
        int i;
        *len = size_;
        for (i = 0; i < size; i++)
            val[i] = lons_[i];
        grib_context_free(c, lons_);
        lons_ = NULL;
        size_ = 0;
        return GRIB_SUCCESS;
    }

    // ECC-1525 Performance: We do not need the values to be decoded
    iter = grib_iterator_new(grib_handle_of_accessor(this), GRIB_GEOITERATOR_NO_VALUES, &ret);
    if (ret != GRIB_SUCCESS) {
        grib_iterator_delete(iter);
        grib_context_log(c, GRIB_LOG_ERROR, "longitudes: Unable to create iterator");
        return ret;
    }

    while (grib_iterator_next(iter, &dummyLat, v++, NULL)) {}
    grib_iterator_delete(iter);

    *len = size;

    return ret;
}

int grib_accessor_longitudes_t::value_count(long* len)
{
    grib_handle* h  = grib_handle_of_accessor(this);
    grib_context* c = context_;
    double* val     = NULL;
    int ret;
    size_t size;
    *len = 0;
    if ((ret = grib_get_size(h, values_, &size)) != GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "longitudes: Unable to get size of %s", values_);
        return ret;
    }
    *len = size;

    // See ECC-1792
    // Give priority to the Grid Section (rather than the Data Section)
    long numberOfDataPoints = 0;
    if (grib_get_long(h, "numberOfDataPoints", &numberOfDataPoints) == GRIB_SUCCESS) {
        *len = numberOfDataPoints;
    }

    if (distinct_) {
        ret = get_distinct(this, &val, len);
        if (ret != GRIB_SUCCESS)
            return ret;
        if (save_) {
            lons_ = val;
            size_ = *len;
        }
        else {
            grib_context_free(c, val);
        }
    }

    return ret;
}

static int compare_doubles_ascending(const void* a, const void* b)
{
    const double* arg1 = (double*)a;
    const double* arg2 = (double*)b;
    if (*arg1 < *arg2)
        return -1;
    else if (*arg1 == *arg2)
        return 0;
    else
        return 1;
}

static int get_distinct(grib_accessor* a, double** val, long* len)
{
    long count = 0;
    double prev;
    double* v       = NULL;
    double* v1      = NULL;
    double dummyLat = 0;
    int ret         = 0;
    size_t size     = *len;
    grib_context* c = a->context_;

    // Performance: We do not need the values to be decoded
    grib_iterator* iter = grib_iterator_new(grib_handle_of_accessor(a), GRIB_GEOITERATOR_NO_VALUES, &ret);
    if (ret != GRIB_SUCCESS) {
        grib_iterator_delete(iter);
        grib_context_log(c, GRIB_LOG_ERROR, "longitudes: Unable to create iterator");
        return ret;
    }
    v = (double*)grib_context_malloc_clear(c, size * sizeof(double));
    if (!v) {
        grib_context_log(c, GRIB_LOG_ERROR, "longitudes: Error allocating %zu bytes", size * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }
    *val = v;

    while (grib_iterator_next(iter, &dummyLat, v++, NULL)) {}
    grib_iterator_delete(iter);
    v = *val;

    qsort(v, *len, sizeof(double), &compare_doubles_ascending);

    v1 = (double*)grib_context_malloc_clear(c, size * sizeof(double));
    if (!v1) {
        grib_context_log(c, GRIB_LOG_ERROR, "longitudes: Error allocating %zu bytes", size * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }

    prev  = v[0];
    v1[0] = prev;
    count = 1;
    for (long i = 1; i < *len; i++) {
        if (v[i] != prev) {
            prev      = v[i];
            v1[count] = prev;
            count++;
        }
    }

    grib_context_free(c, v);

    *val = v1;

    *len = count;
    return GRIB_SUCCESS;
}
