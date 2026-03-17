/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_evaluate.h"

grib_accessor_evaluate_t _grib_accessor_evaluate{};
grib_accessor* grib_accessor_evaluate = &_grib_accessor_evaluate;

void grib_accessor_evaluate_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    arg_ = c; // the expression to be evaluated
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_evaluate_t::unpack_long(long* val, size_t* len)
{
    if (!arg_) return GRIB_INVALID_ARGUMENT;

    grib_handle* h = grib_handle_of_accessor(this);
    grib_expression* e = arg_->get_expression(h, 0);

    int ret = e->evaluate_long(h, val);
    *len    = 1;

    return ret;
}
