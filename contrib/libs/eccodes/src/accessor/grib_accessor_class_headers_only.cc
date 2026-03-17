/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_headers_only.h"

grib_accessor_headers_only_t _grib_accessor_headers_only{};
grib_accessor* grib_accessor_headers_only = &_grib_accessor_headers_only;

void grib_accessor_headers_only_t::init(const long l, grib_arguments* c)
{
    grib_accessor_gen_t::init(l, c);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;
    length_ = 0;
}

int grib_accessor_headers_only_t::unpack_long(long* val, size_t* len)
{
    *val = grib_handle_of_accessor(this)->partial;
    *len = 1;
    return 0;
}

long grib_accessor_headers_only_t::get_native_type()
{
    return GRIB_TYPE_LONG;
}
