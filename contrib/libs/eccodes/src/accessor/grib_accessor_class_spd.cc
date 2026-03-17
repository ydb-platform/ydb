/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_spd.h"

grib_accessor_spd_t _grib_accessor_spd{};
grib_accessor* grib_accessor_spd = &_grib_accessor_spd;

long grib_accessor_spd_t::byte_count()
{
    return length_;
}

long grib_accessor_spd_t::compute_byte_count()
{
    long numberOfBits         = 0;
    long numberOfElements     = 0;

    int ret = grib_get_long(grib_handle_of_accessor(this), numberOfBits_, &numberOfBits);
    if (ret) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s unable to get %s to compute size", name_, numberOfBits_);
        return 0;
    }

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfElements_, &numberOfElements);
    if (ret) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s unable to get %s to compute size", name_, numberOfElements_);
        return 0;
    }
    numberOfElements++;

    return (numberOfBits * numberOfElements + 7) / 8;
}

void grib_accessor_spd_t::init(const long len, grib_arguments* args)
{
    grib_accessor_long_t::init(len, args);
    int n             = 0;
    numberOfBits_     = args->get_name(grib_handle_of_accessor(this), n++);
    numberOfElements_ = args->get_name(grib_handle_of_accessor(this), n++);
    length_           = compute_byte_count();
}

int grib_accessor_spd_t::unpack_long(long* val, size_t* len)
{
    long pos          = offset_ * 8;
    long rlen         = 0;
    long numberOfBits = 0;

    int ret = value_count(&rlen);
    if (ret)
        return ret;

    if (*len < rlen) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Wrong size (%zu) for %s, it contains %ld values", *len, name_, rlen);
        *len = rlen;
        return GRIB_ARRAY_TOO_SMALL;
    }

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfBits_, &numberOfBits);
    if (ret)
        return ret;
    if (numberOfBits > 64) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Invalid number of bits: %ld", numberOfBits);
        return GRIB_DECODING_ERROR;
    }

    for (long i = 0; i < rlen - 1; i++)
        val[i] = grib_decode_unsigned_long(grib_handle_of_accessor(this)->buffer->data, &pos, numberOfBits);

    val[rlen - 1] = grib_decode_signed_longb(grib_handle_of_accessor(this)->buffer->data, &pos, numberOfBits);

    *len = rlen;

    return GRIB_SUCCESS;
}

int grib_accessor_spd_t::pack_long(const long* val, size_t* len)
{
    int ret            = 0;
    long off           = 0;
    long numberOfBits  = 0;
    size_t buflen      = 0;
    unsigned char* buf = NULL;
    unsigned long i    = 0;
    long rlen          = 0;

    ret = value_count(&rlen);
    if (ret)
        return ret;

    if (*len != rlen) {
        ret = grib_set_long(grib_handle_of_accessor(this), numberOfElements_, (*len) - 1);
        if (ret) return ret;
    }

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfBits_, &numberOfBits);
    if (ret)
        return ret;

    buflen = compute_byte_count();
    buf    = (unsigned char*)grib_context_malloc_clear(context_, buflen);

    for (i = 0; i < rlen - 1; i++) {
        grib_encode_unsigned_longb(buf, val[i], &off, numberOfBits);
    }

    grib_encode_signed_longb(buf, val[rlen - 1], &off, numberOfBits);

    grib_buffer_replace(this, buf, buflen, 1, 1);

    grib_context_free(context_, buf);

    *len = rlen;
    return ret;
}

int grib_accessor_spd_t::value_count(long* numberOfElements)
{
    int ret;
    *numberOfElements = 0;

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfElements_, numberOfElements);
    if (ret) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s unable to get %s to compute size", name_, numberOfElements_);
        return ret;
    }
    (*numberOfElements)++;

    return ret;
}

long grib_accessor_spd_t::byte_offset()
{
    return offset_;
}

void grib_accessor_spd_t::update_size(size_t s)
{
    length_ = s;
}

long grib_accessor_spd_t::next_offset()
{
    return byte_offset() + length_;
}
