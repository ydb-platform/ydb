/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_position.h"

grib_accessor_position_t _grib_accessor_position{};
grib_accessor* grib_accessor_position = &_grib_accessor_position;

void grib_accessor_position_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
}

long grib_accessor_position_t::get_native_type()
{
    return GRIB_TYPE_LONG;
}

void grib_accessor_position_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_long(this, NULL);
}

int grib_accessor_position_t::unpack_long(long* val, size_t* len)
{
    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it contains %d values ", name_, 1);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }
    *val = offset_;
    *len = 1;
    return GRIB_SUCCESS;
}

// static int compare(grib_accessor* a, grib_accessor* b)
// {
//     if (offset_ != b->offset)
//         return GRIB_OFFSET_MISMATCH;
//     return GRIB_SUCCESS;
// }
