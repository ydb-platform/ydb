/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_padding.h"

grib_accessor_padding_t _grib_accessor_padding{};
grib_accessor* grib_accessor_padding = &_grib_accessor_padding;

void grib_accessor_padding_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_bytes_t::init(len, arg);
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_padding_t::compare(grib_accessor* b)
{
    if (length_ != b->length_)
        return GRIB_COUNT_MISMATCH;
    return GRIB_SUCCESS;
}

void grib_accessor_padding_t::update_size(size_t new_size)
{
    length_ = new_size;
}

void grib_accessor_padding_t::resize(size_t new_size)
{
    void* zero = grib_context_malloc_clear(context_, new_size);

    grib_buffer_replace(this, (const unsigned char*)zero, new_size,
                        /*update_lengths=*/1, /*update_paddings=*/0);
    grib_context_free(context_, zero);

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "grib_accessor_padding::resize new_size=%zu length_ =%ld %s %s",
                     new_size, length_, class_name_, name_);
    ECCODES_ASSERT(new_size == length_);
}

int grib_accessor_padding_t::value_count(long* c)
{
    *c = length_;
    return 0;
}

long grib_accessor_padding_t::byte_count()
{
    return length_;
}

size_t grib_accessor_padding_t::string_length()
{
    return (size_t)length_;
}
