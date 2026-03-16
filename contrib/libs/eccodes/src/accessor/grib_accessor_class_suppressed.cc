/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_suppressed.h"

grib_accessor_suppressed_t _grib_accessor_suppressed{};
grib_accessor* grib_accessor_suppressed = &_grib_accessor_suppressed;

void grib_accessor_suppressed_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    args_ = c;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    length_ = 0;
}

void grib_accessor_suppressed_t::log_message()
{
    int i                            = 0;
    grib_handle* hand                = grib_handle_of_accessor(this);

    grib_context_log(context_, GRIB_LOG_ERROR,
                     "key '%s' is unavailable in this version.", name_);
    grib_context_log(context_, GRIB_LOG_ERROR,
                     "Please use the following key(s):");
    while (args_->get_name(hand, i)) {
        grib_context_log(context_, GRIB_LOG_ERROR, "\t- %s",
                         args_->get_name(hand, i));
        i++;
    }
}

int grib_accessor_suppressed_t::unpack_string(char* val, size_t* len)
{
    log_message();
    return GRIB_NOT_FOUND;
}

int grib_accessor_suppressed_t::unpack_long(long* val, size_t* len)
{
    log_message();
    return GRIB_NOT_FOUND;
}

int grib_accessor_suppressed_t::unpack_double(double* val, size_t* len)
{
    log_message();
    return GRIB_NOT_FOUND;
}

int grib_accessor_suppressed_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

long grib_accessor_suppressed_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}
