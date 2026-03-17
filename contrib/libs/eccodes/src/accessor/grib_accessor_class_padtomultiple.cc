/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_padtomultiple.h"

grib_accessor_padtomultiple_t _grib_accessor_padtomultiple{};
grib_accessor* grib_accessor_padtomultiple = &_grib_accessor_padtomultiple;

size_t grib_accessor_padtomultiple_t::preferred_size(int from_handle)
{
    long padding  = 0;
    long begin    = 0;
    long multiple = 0;

    begin_->evaluate_long(grib_handle_of_accessor(this), &begin);
    multiple_->evaluate_long(grib_handle_of_accessor(this), &multiple);

    padding = offset_ - begin;
    padding = ((padding + multiple - 1) / multiple) * multiple - padding;

    return padding == 0 ? multiple : padding;
}

void grib_accessor_padtomultiple_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_padding_t::init(len, arg);

    begin_    = arg->get_expression(grib_handle_of_accessor(this), 0);
    multiple_ = arg->get_expression(grib_handle_of_accessor(this), 1);
    length_   = preferred_size(1);
}
