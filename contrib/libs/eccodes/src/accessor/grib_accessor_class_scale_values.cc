/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_scale_values.h"

grib_accessor_scale_values_t _grib_accessor_scale_values{};
grib_accessor* grib_accessor_scale_values = &_grib_accessor_scale_values;

void grib_accessor_scale_values_t::init(const long l, grib_arguments* args)
{
    grib_accessor_double_t::init(l, args);
    int n         = 0;
    values_       = args->get_name(grib_handle_of_accessor(this), n++);
    missingValue_ = args->get_name(grib_handle_of_accessor(this), n++);
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    length_ = 0;
}

int grib_accessor_scale_values_t::unpack_double(double* val, size_t* len)
{
    int ret = GRIB_SUCCESS;
    *val    = 1;
    *len    = 1;
    return ret;
}

int grib_accessor_scale_values_t::pack_double(const double* val, size_t* len)
{
    double* values            = NULL;
    double missingValue       = 0;
    long missingValuesPresent = 0;
    size_t size               = 0;
    int ret = 0, i = 0;
    const grib_context* c = context_;
    grib_handle* h        = grib_handle_of_accessor(this);

    if (*val == 1)
        return GRIB_SUCCESS;

    if ((ret = grib_get_double_internal(h, missingValue_, &missingValue)) != GRIB_SUCCESS) {
        return ret;
    }
    if ((ret = grib_get_long_internal(h, "missingValuesPresent", &missingValuesPresent)) != GRIB_SUCCESS) {
        return ret;
    }

    if ((ret = grib_get_size(h, values_, &size)) != GRIB_SUCCESS)
        return ret;

    values = (double*)grib_context_malloc(c, size * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;

    if ((ret = grib_get_double_array_internal(h, values_, values, &size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }

    for (i = 0; i < size; i++) {
        if (missingValuesPresent) {
            if (values[i] != missingValue)
                values[i] *= *val;
        }
        else {
            values[i] *= *val;
        }
    }

    if ((ret = grib_set_double_array_internal(h, values_, values, size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }

    grib_context_free(c, values);

    return GRIB_SUCCESS;
}
