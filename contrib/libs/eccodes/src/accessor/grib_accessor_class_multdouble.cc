/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_multdouble.h"

grib_accessor_multdouble_t _grib_accessor_multdouble{};
grib_accessor* grib_accessor_multdouble = &_grib_accessor_multdouble;

void grib_accessor_multdouble_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n = 0;

    val_        = c->get_name(grib_handle_of_accessor(this), n++);
    multiplier_ = c->get_double(grib_handle_of_accessor(this), n++);
}

int grib_accessor_multdouble_t::unpack_double(double* val, size_t* len)
{
    int ret      = GRIB_SUCCESS;
    double value = 0;

    ret = grib_get_double_internal(grib_handle_of_accessor(this), val_, &value);
    if (ret != GRIB_SUCCESS)
        return ret;

    *val = value * multiplier_;

    *len = 1;
    return GRIB_SUCCESS;
}
