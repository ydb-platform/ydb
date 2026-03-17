/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_size.h"

grib_accessor_size_t _grib_accessor_size{};
grib_accessor* grib_accessor_size = &_grib_accessor_size;

void grib_accessor_size_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    accessor_ = c->get_name(grib_handle_of_accessor(this), 0);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    length_ = 0;
}

int grib_accessor_size_t::unpack_long(long* val, size_t* len)
{
    size_t size = 0;
    int ret     = grib_get_size(grib_handle_of_accessor(this), accessor_, &size);
    *val        = (long)size;
    *len        = 1;
    return ret;
}
