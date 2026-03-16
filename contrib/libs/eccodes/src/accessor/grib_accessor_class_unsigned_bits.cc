/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_unsigned_bits.h"

grib_accessor_unsigned_bits_t _grib_accessor_unsigned_bits{};
grib_accessor* grib_accessor_unsigned_bits = &_grib_accessor_unsigned_bits;

long grib_accessor_unsigned_bits_t::compute_byte_count()
{
    long numberOfBits;
    long numberOfElements;
    int ret = 0;

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfBits_, &numberOfBits);
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

    return (numberOfBits * numberOfElements + 7) / 8;
}

void grib_accessor_unsigned_bits_t::init(const long len, grib_arguments* args)
{
    grib_accessor_long_t::init(len, args);
    int n             = 0;
    numberOfBits_     = args->get_name(grib_handle_of_accessor(this), n++);
    numberOfElements_ = args->get_name(grib_handle_of_accessor(this), n++);
    length_           = compute_byte_count();
}

int grib_accessor_unsigned_bits_t::unpack_long(long* val, size_t* len)
{
    int ret           = 0;
    long pos          = offset_ * 8;
    long rlen         = 0;
    long numberOfBits = 0;

    ret = value_count(&rlen);
    if (ret)
        return ret;

    if (*len < rlen) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Wrong size (%ld) for %s, it contains %ld values", *len, name_, rlen);
        *len = rlen;
        return GRIB_ARRAY_TOO_SMALL;
    }

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfBits_, &numberOfBits);
    if (ret)
        return ret;
    if (numberOfBits == 0) {
        int i;
        for (i = 0; i < rlen; i++)
            val[i] = 0;
        return GRIB_SUCCESS;
    }

    grib_decode_long_array(grib_handle_of_accessor(this)->buffer->data, &pos, numberOfBits, rlen, val);

    *len = rlen;

    return GRIB_SUCCESS;
}

int grib_accessor_unsigned_bits_t::pack_long(const long* val, size_t* len)
{
    int ret            = 0;
    long off           = 0;
    long numberOfBits  = 0;
    size_t buflen      = 0;
    unsigned char* buf = NULL;
    unsigned long i    = 0;
    long rlen          = 0;
    ret                = value_count(&rlen);
    if (ret) return ret;

    /*
    if(*len < rlen)
    {
        grib_context_log(context_ , GRIB_LOG_ERROR,
            "Wrong size for %s it contains %d values ", name_ , rlen );
        return GRIB_ARRAY_TOO_SMALL;
    }
     */
    if (*len != rlen)
        ret = grib_set_long(grib_handle_of_accessor(this), numberOfElements_, *len);
    if (ret) return ret;

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfBits_, &numberOfBits);
    if (ret) return ret;
    if (numberOfBits == 0) {
        grib_buffer_replace(this, NULL, 0, 1, 1);
        return GRIB_SUCCESS;
    }

    buflen = compute_byte_count();
    buf    = (unsigned char*)grib_context_malloc_clear(context_, buflen + sizeof(long));

    for (i = 0; i < *len; i++)
        grib_encode_unsigned_longb(buf, val[i], &off, numberOfBits);

    grib_buffer_replace(this, buf, buflen, 1, 1);

    grib_context_free(context_, buf);

    return ret;
}

long grib_accessor_unsigned_bits_t::byte_count()
{
    return length_;
}

int grib_accessor_unsigned_bits_t::value_count(long* numberOfElements)
{
    int ret;
    *numberOfElements = 0;

    ret = grib_get_long(grib_handle_of_accessor(this), numberOfElements_, numberOfElements);
    if (ret) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s unable to get %s to compute size", name_, numberOfElements_);
    }

    return ret;
}

long grib_accessor_unsigned_bits_t::byte_offset()
{
    return offset_;
}

void grib_accessor_unsigned_bits_t::update_size(size_t s)
{
    length_ = s;
}

long grib_accessor_unsigned_bits_t::next_offset()
{
    return byte_offset() + length_;
}
