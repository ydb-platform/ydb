/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_to_string.h"

grib_accessor_to_string_t _grib_accessor_to_string{};
grib_accessor* grib_accessor_to_string = &_grib_accessor_to_string;

void grib_accessor_to_string_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    grib_handle* hand = grib_handle_of_accessor(this);

    key_        = arg->get_name(hand, 0);
    start_      = arg->get_long(hand, 1);
    str_length_ = arg->get_long(hand, 2);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    grib_accessor_gen_t::length_ = 0;
}

int grib_accessor_to_string_t::value_count(long* count)
{
    size_t size = 0;

    int err = grib_get_size(grib_handle_of_accessor(this), key_, &size);
    *count  = size;

    return err;
}

size_t grib_accessor_to_string_t::string_length()
{
    if (str_length_)
        return str_length_;

    size_t size = 0;
    grib_get_string_length(grib_handle_of_accessor(this), key_, &size);
    return size;
}

void grib_accessor_to_string_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

long grib_accessor_to_string_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_to_string_t::unpack_string(char* val, size_t* len)
{
    int err        = 0;
    char buff[512] = {0,};
    size_t length = string_length();

    if (*len < length + 1) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, length + 1, *len);
        *len = length + 1;
        return GRIB_BUFFER_TOO_SMALL;
    }

    size_t size = sizeof(buff);
    err         = grib_get_string(grib_handle_of_accessor(this), key_, buff, &size);
    if (err)
        return err;
    if (length > size) {
        /*err=GRIB_STRING_TOO_SMALL;*/
        length = size;
    }

    memcpy(val, buff + start_, length);

    val[length] = 0;
    *len        = length;
    return GRIB_SUCCESS;
}

int grib_accessor_to_string_t::unpack_long(long* v, size_t* len)
{
    char val[1024] = {0,};
    size_t l   = sizeof(val);
    char* last = NULL;
    int err    = unpack_string(val, &l);

    if (err)
        return err;

    *v = strtol(val, &last, 10);
    if (*last) {
        err = GRIB_WRONG_CONVERSION;
    }

    return err;
}

int grib_accessor_to_string_t::unpack_double(double* v, size_t* len)
{
    size_t l = 1;
    long val = 0;
    int err  = unpack_long(&val, &l);

    *v = (double)val;
    return err;
}

long grib_accessor_to_string_t::next_offset()
{
    return offset_ + grib_accessor_gen_t::length_;
}
