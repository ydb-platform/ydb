/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_pad.h"

grib_accessor_pad_t _grib_accessor_pad{};
grib_accessor* grib_accessor_pad = &_grib_accessor_pad;

void grib_accessor_pad_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_padding_t::init(len, arg);

    expression_ = arg->get_expression(grib_handle_of_accessor(this), 0);
    length_     = preferred_size(1);
}

size_t grib_accessor_pad_t::preferred_size(int from_handle)
{
    long length = 0;

    expression_->evaluate_long(grib_handle_of_accessor(this), &length);

    return length > 0 ? length : 0;
}
