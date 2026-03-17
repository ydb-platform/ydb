/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_gds_is_present.h"

grib_accessor_gds_is_present_t _grib_accessor_gds_is_present{};
grib_accessor* grib_accessor_gds_is_present = &_grib_accessor_gds_is_present;

void grib_accessor_gds_is_present_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n            = 0;
    grib_handle* h   = grib_handle_of_accessor(this);
    gds_present_     = c->get_name(h, n++);
    grid_definition_ = c->get_name(h, n++);
    bitmap_present_  = c->get_name(h, n++);
    values_          = c->get_name(h, n++);

    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;
    length_ = 0;
}

int grib_accessor_gds_is_present_t::pack_long(const long* val, size_t* len)
{
    long missing    = 255;
    int ret         = 0;
    size_t size     = 0;
    double* values  = NULL;
    grib_context* c = context_;
    grib_handle* h  = grib_handle_of_accessor(this);

    if (*val != 1)
        return GRIB_NOT_IMPLEMENTED;

    if ((ret = grib_get_size(h, values_, &size)) != GRIB_SUCCESS)
        return ret;

    values = (double*)grib_context_malloc(c, size * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;

    if ((ret = grib_get_double_array_internal(h, values_, values, &size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }

    if ((ret = grib_set_long_internal(h, gds_present_, *val)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_long_internal(h, bitmap_present_, *val)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_long_internal(h, grid_definition_, missing)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_double_array_internal(h, values_, values, size)) != GRIB_SUCCESS)
        return ret;

    grib_context_free(c, values);

    return GRIB_SUCCESS;
}

int grib_accessor_gds_is_present_t::unpack_long(long* val, size_t* len)
{
    int ret        = 0;
    grib_handle* h = grib_handle_of_accessor(this);

    if ((ret = grib_get_long_internal(h, gds_present_, val)) != GRIB_SUCCESS)
        return ret;

    *len = 1;

    return GRIB_SUCCESS;
}
