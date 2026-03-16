/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_count_file.h"

grib_accessor_count_file_t _grib_accessor_count_file{};
grib_accessor* grib_accessor_count_file = &_grib_accessor_count_file;

void grib_accessor_count_file_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    length_ = 0;
}

int grib_accessor_count_file_t::unpack_long(long* val, size_t* len)
{
    *val = grib_context_get_handle_file_count(context_);
    *len = 1;
    return 0;
}
