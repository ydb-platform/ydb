/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_transient_darray.h"

grib_accessor_transient_darray_t _grib_accessor_transient_darray{};
grib_accessor* grib_accessor_transient_darray = &_grib_accessor_transient_darray;

void grib_accessor_transient_darray_t::init(const long length, grib_arguments* args)
{
    grib_accessor_gen_t::init(length, args);
    arr_    = NULL;
    type_   = GRIB_TYPE_DOUBLE;
    length_ = 0;
}

void grib_accessor_transient_darray_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_double(this, NULL);
}

int grib_accessor_transient_darray_t::pack_double(const double* val, size_t* len)
{
    if (arr_)
        grib_darray_delete(arr_);
    arr_ = grib_darray_new(*len, 10);

    for (size_t i = 0; i < *len; i++)
        grib_darray_push(arr_, val[i]);

    return GRIB_SUCCESS;
}

int grib_accessor_transient_darray_t::pack_long(const long* val, size_t* len)
{
    if (arr_)
        grib_darray_delete(arr_);
    arr_ = grib_darray_new(*len, 10);

    for (size_t i = 0; i < *len; i++)
        grib_darray_push(arr_, (double)val[i]);

    return GRIB_SUCCESS;
}

int grib_accessor_transient_darray_t::unpack_double(double* val, size_t* len)
{
    long count = 0;

    value_count(&count);

    if (*len < count) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s (setting %ld, required %ld) ", name_, *len, count);
        return GRIB_ARRAY_TOO_SMALL;
    }

    *len = count;
    for (size_t i = 0; i < *len; i++)
        val[i] = arr_->v[i];

    return GRIB_SUCCESS;
}

int grib_accessor_transient_darray_t::unpack_long(long* val, size_t* len)
{
    long count = 0;

    value_count(&count);

    if (*len < count) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s (setting %ld, required %ld) ", name_, *len, count);
        return GRIB_ARRAY_TOO_SMALL;
    }

    *len = count;
    for (size_t i = 0; i < *len; i++)
        val[i] = (long)arr_->v[i];

    return GRIB_SUCCESS;
}

void grib_accessor_transient_darray_t::destroy(grib_context* c)
{
    if (arr_)
        grib_darray_delete(arr_);
    grib_accessor_gen_t::destroy(c);
}

int grib_accessor_transient_darray_t::value_count(long* count)
{
    if (arr_)
        *count = grib_darray_used_size(arr_);
    else
        *count = 0;

    return 0;
}

long grib_accessor_transient_darray_t::get_native_type()
{
    return type_;
}
