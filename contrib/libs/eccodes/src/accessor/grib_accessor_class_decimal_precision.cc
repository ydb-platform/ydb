/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_decimal_precision.h"

grib_accessor_decimal_precision_t _grib_accessor_decimal_precision{};
grib_accessor* grib_accessor_decimal_precision = &_grib_accessor_decimal_precision;

void grib_accessor_decimal_precision_t::init(const long l, grib_arguments* args)
{
    grib_accessor_long_t::init(l, args);
    int n = 0;

    bits_per_value_       = args->get_name(grib_handle_of_accessor(this), n++);
    decimal_scale_factor_ = args->get_name(grib_handle_of_accessor(this), n++);
    changing_precision_   = args->get_name(grib_handle_of_accessor(this), n++);
    values_               = args->get_name(grib_handle_of_accessor(this), n++);

    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    length_ = 0;
}

int grib_accessor_decimal_precision_t::unpack_long(long* val, size_t* len)
{
    int ret        = 0;
    grib_handle* h = grib_handle_of_accessor(this);

    if ((ret = grib_get_long_internal(h, decimal_scale_factor_, val)) != GRIB_SUCCESS)
        return ret;

    *len = 1;
    return ret;
}

int grib_accessor_decimal_precision_t::pack_long(const long* val, size_t* len)
{
    long bitsPerValue = 0;
    double* values    = NULL;
    size_t size       = 0;
    int ret           = 0;
    grib_context* c   = context_;
    grib_handle* h    = grib_handle_of_accessor(this);

    if (!values_) {
        if ((ret = grib_set_long_internal(h, bits_per_value_, 0)) != GRIB_SUCCESS)
            return ret;

        if ((ret = grib_set_long_internal(h, decimal_scale_factor_, *val)) != GRIB_SUCCESS)
            return ret;

        if ((ret = grib_set_long_internal(h, changing_precision_, 1)) != GRIB_SUCCESS) {
            grib_context_free(c, values);
            return ret;
        }

        return GRIB_SUCCESS;
    }

    if ((ret = grib_get_size(h, values_, &size)) != GRIB_SUCCESS)
        return ret;

    values = (double*)grib_context_malloc(c, size * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;

    if ((ret = grib_get_double_array_internal(h, values_, values, &size)) != GRIB_SUCCESS) {
        grib_context_buffer_free(c, values);
        return ret;
    }

    if ((ret = grib_set_long_internal(h, decimal_scale_factor_, *val)) != GRIB_SUCCESS) {
        grib_context_buffer_free(c, values);
        return ret;
    }

    if ((ret = grib_set_long_internal(h, bits_per_value_, bitsPerValue)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }

    if ((ret = grib_set_long_internal(h, changing_precision_, 1)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }

    if ((ret = grib_set_double_array_internal(h, values_, values, size)) != GRIB_SUCCESS) {
        grib_context_buffer_free(c, values);
        return ret;
    }

    grib_context_free(c, values);

    return GRIB_SUCCESS;
}
