/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_bits.h"
#include "ecc_numeric_limits.h"


grib_accessor_bits_t _grib_accessor_bits{};
grib_accessor* grib_accessor_bits = &_grib_accessor_bits;

void grib_accessor_bits_t::init(const long l, grib_arguments* c)
{
    grib_accessor_gen_t::init(l, c);
    grib_handle* hand  = grib_handle_of_accessor(this);
    grib_expression* e = NULL;
    int n              = 0;
    referenceValue_    = 0;

    argument_ = c->get_name(hand, n++);
    start_    = c->get_long(hand, n++);
    len_      = c->get_long(hand, n++);
    e         = c->get_expression(hand, n++);
    if (e) {
        e->evaluate_double(hand, &(referenceValue_));
        referenceValuePresent_ = 1;
    }
    else {
        referenceValuePresent_ = 0;
    }
    scale_ = 1;
    if (referenceValuePresent_) {
        scale_ = c->get_double(hand, n++);
    }

    ECCODES_ASSERT(len_ <= sizeof(long) * 8);

    length_ = 0;
}

int grib_accessor_bits_t::unpack_long(long* val, size_t* len)
{
    grib_accessor* x = NULL;
    unsigned char* p = NULL;
    grib_handle* h   = grib_handle_of_accessor(this);
    long start, length;
    int ret = 0;

    if (*len < 1)
        return GRIB_WRONG_ARRAY_SIZE;

    start  = start_;
    length = len_;

    x = grib_find_accessor(grib_handle_of_accessor(this), argument_);
    if (!x)
        return GRIB_NOT_FOUND;

    p    = h->buffer->data + x->byte_offset();
    *val = grib_decode_unsigned_long(p, &start, length);

    *len = 1;

    return ret;
}

int grib_accessor_bits_t::unpack_double(double* val, size_t* len)
{
    grib_accessor* x = NULL;
    unsigned char* p = NULL;
    grib_handle* h   = grib_handle_of_accessor(this);
    long start, length;
    int ret = 0;

    if (*len < 1)
        return GRIB_WRONG_ARRAY_SIZE;

    start  = start_;
    length = len_;

    x = grib_find_accessor(grib_handle_of_accessor(this), argument_);
    if (!x)
        return GRIB_NOT_FOUND;

    p    = h->buffer->data + x->byte_offset();
    *val = grib_decode_unsigned_long(p, &start, length);

    *val = ((long)*val + referenceValue_) / scale_;

    *len = 1;

    return ret;
}

int grib_accessor_bits_t::pack_double(const double* val, size_t* len)
{
    grib_accessor* x = NULL;
    grib_handle* h   = grib_handle_of_accessor(this);
    unsigned char* p = NULL;
    long start, length, lval;

    if (*len != 1)
        return GRIB_WRONG_ARRAY_SIZE;

    start  = start_;
    length = len_;

    x = grib_find_accessor(grib_handle_of_accessor(this), argument_);
    if (!x)
        return GRIB_NOT_FOUND;

    p    = h->buffer->data + x->byte_offset();
    lval = round(*val * scale_) - referenceValue_;
    return grib_encode_unsigned_longb(p, lval, &start, length);
}

int grib_accessor_bits_t::pack_long(const long* val, size_t* len)
{
    grib_accessor* x = NULL;
    grib_handle* h   = grib_handle_of_accessor(this);
    unsigned char* p = NULL;
    long start, length, maxval;

    if (*len != 1)
        return GRIB_WRONG_ARRAY_SIZE;

    if (get_native_type() == GRIB_TYPE_DOUBLE) {
        /* ECC-402 */
        const double dVal = (double)(*val);
        return pack_double(&dVal, len);
    }

    start  = start_;
    length = len_;

    x = grib_find_accessor(grib_handle_of_accessor(this), argument_);
    if (!x)
        return GRIB_NOT_FOUND;

    /* Check the input value */
    if (*val < 0) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "key=%s: value cannot be negative", name_);
        return GRIB_ENCODING_ERROR;
    }

#ifdef DEBUG
    {
        const long numbits = (x->length_) * 8;
        if (start + length > numbits) {
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "grib_accessor_bits::pack_long: key=%s (x=%s): "
                             "Invalid start/length. x->length=%ld, start=%ld, length=%ld",
                             name_, x->name_, numbits, start, length);
            return GRIB_ENCODING_ERROR;
        }
    }
#endif

    maxval = NumericLimits<unsigned long>::max(length);
    if (*val > maxval) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "key=%s: Trying to encode value of %ld but the maximum allowable value is %ld (number of bits=%ld)",
                         name_, *val, maxval, length);
        return GRIB_ENCODING_ERROR;
    }

    p = h->buffer->data + x->byte_offset();
    return grib_encode_unsigned_longb(p, *val, &start, length);
}

long grib_accessor_bits_t::get_native_type()
{
    int type = GRIB_TYPE_BYTES;

    if (flags_ & GRIB_ACCESSOR_FLAG_STRING_TYPE)
        type = GRIB_TYPE_STRING;

    if (flags_ & GRIB_ACCESSOR_FLAG_LONG_TYPE)
        type = GRIB_TYPE_LONG;

    if (referenceValuePresent_)
        type = GRIB_TYPE_DOUBLE;

    return type;
}

int grib_accessor_bits_t::unpack_string(char* v, size_t* len)
{
    int ret     = 0;
    double dval = 0;
    long lval   = 0;
    size_t llen = 1;

    switch (get_native_type()) {
        case GRIB_TYPE_LONG:
            ret = unpack_long(&lval, &llen);
            snprintf(v, 64, "%ld", lval);
            *len = strlen(v);
            break;

        case GRIB_TYPE_DOUBLE:
            ret = unpack_double(&dval, &llen);
            snprintf(v, 64, "%g", dval);
            *len = strlen(v);
            break;

        default:
            ret = grib_accessor_gen_t::unpack_string(v, len);
    }
    return ret;
}

long grib_accessor_bits_t::byte_count()
{
    grib_context_log(context_, GRIB_LOG_DEBUG, "byte_count of %s = %ld", name_, length_);
    return length_;
}

int grib_accessor_bits_t::unpack_bytes(unsigned char* buffer, size_t* len)
{
    if (*len < length_) {
        *len = length_;
        return GRIB_ARRAY_TOO_SMALL;
    }
    *len = length_;

    memcpy(buffer, grib_handle_of_accessor(this)->buffer->data + offset_, *len);

    return GRIB_SUCCESS;
}
