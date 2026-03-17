/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_gts_header.h"

grib_accessor_gts_header_t _grib_accessor_gts_header{};
grib_accessor* grib_accessor_gts_header = &_grib_accessor_gts_header;

void grib_accessor_gts_header_t::init(const long l, grib_arguments* c)
{
    grib_accessor_ascii_t::init(l, c);
    gts_offset_ = c ? c->get_long(grib_handle_of_accessor(this), 0) : 0;
    gts_length_ = c ? c->get_long(grib_handle_of_accessor(this), 1) : 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_gts_header_t::unpack_string(char* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);
    int offset     = 0;
    size_t length  = 0;

    if (h->gts_header == NULL || h->gts_header_len < 8) {
        if (*len < 8)
            return GRIB_BUFFER_TOO_SMALL;
        snprintf(val, 1024, "missing");
        return GRIB_SUCCESS;
    }
    if (*len < h->gts_header_len)
        return GRIB_BUFFER_TOO_SMALL;

    offset = gts_offset_ > 0 ? gts_offset_ : 0;
    length = gts_length_ > 0 ? gts_length_ : h->gts_header_len;

    memcpy(val, h->gts_header + offset, length);

    *len = length;

    return GRIB_SUCCESS;
}

int grib_accessor_gts_header_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_gts_header_t::string_length()
{
    const grib_handle* h = grib_handle_of_accessor(this);
    return h->gts_header_len;
}
