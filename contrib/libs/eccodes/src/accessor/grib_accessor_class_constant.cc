/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_constant.h"

grib_accessor_constant_t _grib_accessor_constant{};
grib_accessor* grib_accessor_constant = &_grib_accessor_constant;

void grib_accessor_constant_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_variable_t::init(len, arg);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}
