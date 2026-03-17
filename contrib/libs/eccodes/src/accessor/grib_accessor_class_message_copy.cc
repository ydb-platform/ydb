/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_message_copy.h"

grib_accessor_message_copy_t _grib_accessor_message_copy{};
grib_accessor* grib_accessor_message_copy = &_grib_accessor_message_copy;

void grib_accessor_message_copy_t::init(const long length, grib_arguments* args)
{
    grib_accessor_gen_t::init(length, args);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    length_ = 0;
}

void grib_accessor_message_copy_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

long grib_accessor_message_copy_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_message_copy_t::unpack_string(char* val, size_t* len)
{
    size_t slen = grib_handle_of_accessor(this)->buffer->ulength;
    size_t i;
    unsigned char* v = 0;

    if (*len < slen) {
        return GRIB_ARRAY_TOO_SMALL;
    }
    v = grib_handle_of_accessor(this)->buffer->data;
    /* replace unprintable characters with space */
    for (i = 0; i < slen; i++)
        if (v[i] > 126)
            v[i] = 32;
    memcpy(val, grib_handle_of_accessor(this)->buffer->data, slen);
    val[i] = 0;

    *len = slen;

    return GRIB_SUCCESS;
}

size_t grib_accessor_message_copy_t::string_length()
{
    return grib_handle_of_accessor(this)->buffer->ulength;
}

long grib_accessor_message_copy_t::byte_count()
{
    return length_;
}
