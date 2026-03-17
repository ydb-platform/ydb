/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_uint8.h"

grib_accessor_uint8_t _grib_accessor_uint8{};
grib_accessor* grib_accessor_uint8 = &_grib_accessor_uint8;

int grib_accessor_uint8_t::unpack_long(long* val, size_t* len)
{
    long value                = 0;
    long pos                  = offset_;
    const unsigned char* data = grib_handle_of_accessor(this)->buffer->data;

    if (*len < 1) {
        return GRIB_ARRAY_TOO_SMALL;
    }

    value = data[pos];

    *val = value;
    *len = 1;
    return GRIB_SUCCESS;
}

long grib_accessor_uint8_t::get_native_type()
{
    return GRIB_TYPE_LONG;
}
