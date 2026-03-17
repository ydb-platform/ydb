/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_signed.h"
#include "ecc_numeric_limits.h"

grib_accessor_signed_t _grib_accessor_signed{};
grib_accessor* grib_accessor_signed = &_grib_accessor_signed;

void grib_accessor_signed_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_long_t::init(len, arg);
    long count = 0;

    arg_ = arg;
    value_count(&count);
    length_ = len * count;
    nbytes_ = len;
    ECCODES_ASSERT(length_ >= 0);
}

void grib_accessor_signed_t::dump(eccodes::Dumper* dumper)
{
    long rlen = 0;
    value_count(&rlen);
    if (rlen == 1)
        dumper->dump_long(this, NULL);
    else
        dumper->dump_values(this);
}

static const long ones[] = {
    0,
    -0x7f,
    -0x7fff,
    -0x7fffff,
    -0x7fffffff,
};

int grib_accessor_signed_t::unpack_long(long* val, size_t* len)
{
    unsigned long rlen = 0;
    int err            = 0;
    long count         = 0;
    unsigned long i    = 0;
    grib_handle* hand  = grib_handle_of_accessor(this);
    long pos           = offset_;
    long missing       = 0;

    err = value_count(&count);
    if (err)
        return err;
    rlen = count;

    if (*len < rlen) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it contains %lu values", name_, rlen);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if (flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) {
        ECCODES_ASSERT(nbytes_ <= 4);
        missing = ones[nbytes_];
    }

    for (i = 0; i < rlen; i++) {
        val[i] = (long)grib_decode_signed_long(hand->buffer->data, pos, nbytes_);
        if (missing)
            if (val[i] == missing)
                val[i] = GRIB_MISSING_LONG;
        pos += nbytes_;
    }

    *len = rlen;
    return GRIB_SUCCESS;
}

int grib_accessor_signed_t::pack_long(const long* val, size_t* len)
{
    int ret            = 0;
    long off           = 0;
    unsigned long rlen = 0;
    long count         = 0;
    size_t buflen      = 0;
    unsigned char* buf = NULL;
    unsigned long i    = 0;
    long missing       = 0;

    int err = value_count(&count);
    if (err)
        return err;
    rlen = count;

    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it contains %d values", name_, 1);
        len[0] = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if (flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) {
        ECCODES_ASSERT(nbytes_ <= 4);
        missing = ones[nbytes_];
    }

    if (rlen == 1) {
        long v = val[0];
        if (missing) {
            if (v == GRIB_MISSING_LONG)
                v = missing;
        }
        else {
            // ECC-1605: Check overflow/underflow
            const int nbits   = nbytes_ * 8;
            const long minval = NumericLimits<long>::min(nbits);
            const long maxval = NumericLimits<long>::max(nbits);
            // printf("  key=%s: v=%ld  (minval=%ld  maxval=%ld)\n", name_ , v, minval, maxval);
            if (v > maxval || v < minval) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "Key \"%s\": Trying to encode value of %ld but the allowable range is %ld to %ld (number of bits=%d)",
                                 name_, v, minval, maxval, nbits);
                return GRIB_ENCODING_ERROR;
            }
        }

        off = offset_;
        ret = grib_encode_signed_long(grib_handle_of_accessor(this)->buffer->data, v, off, length_);
        if (ret == GRIB_SUCCESS)
            len[0] = 1;
        if (*len > 1)
            grib_context_log(context_, GRIB_LOG_WARNING, "grib_accessor_signed_t : Trying to pack %d values in a scalar %s, packing first value", *len, name_);
        len[0] = 1;
        return ret;
    }

    /* TODO: We assume that there are no missing values if there are more that 1 value */

    buflen = *len * length_;

    buf = (unsigned char*)grib_context_malloc(context_, buflen);

    for (i = 0; i < *len; i++) {
        grib_encode_signed_long(buf, val[i], off, length_);
        off += length_;
    }
    ret = grib_set_long_internal(grib_handle_of_accessor(this), arg_->get_name(parent_->h, 0), *len);

    if (ret == GRIB_SUCCESS)
        grib_buffer_replace(this, buf, buflen, 1, 1);
    else
        *len = 0;

    grib_context_free(context_, buf);
    return ret;
}

long grib_accessor_signed_t::byte_count()
{
    return length_;
}

int grib_accessor_signed_t::value_count(long* len)
{
    *len = 0;
    if (!arg_) {
        *len = 1;
        return 0;
    }
    return grib_get_long_internal(grib_handle_of_accessor(this), arg_->get_name(parent_->h, 0), len);
}

long grib_accessor_signed_t::byte_offset()
{
    return offset_;
}

void grib_accessor_signed_t::update_size(size_t s)
{
    length_ = s;
    ECCODES_ASSERT(length_ >= 0);
}

long grib_accessor_signed_t::next_offset()
{
    return byte_offset() + byte_count();
}

int grib_accessor_signed_t::is_missing()
{
    unsigned char ff     = 0xff;
    unsigned long offset = offset_;
    const grib_handle* hand    = grib_handle_of_accessor(this);

    if (length_ == 0) {
        ECCODES_ASSERT(vvalue_ != NULL);
        return vvalue_->missing;
    }

    for (long i = 0; i < length_; i++) {
        if (hand->buffer->data[offset] != ff)
            return 0;
        offset++;
    }

    return 1;
}
