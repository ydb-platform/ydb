/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_ieeefloat.h"

grib_accessor_ieeefloat_t _grib_accessor_ieeefloat{};
grib_accessor* grib_accessor_ieeefloat = &_grib_accessor_ieeefloat;

void grib_accessor_ieeefloat_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_double_t::init(len, arg);
    long count = 0;
    arg_       = arg;
    value_count(&count);
    length_ = 4 * count;
    ECCODES_ASSERT(length_ >= 0);
}

int grib_accessor_ieeefloat_t::value_count(long* len)
{
    *len = 0;

    if (!arg_) {
        *len = 1;
        return 0;
    }
    return grib_get_long_internal(grib_handle_of_accessor(this), arg_->get_name(parent_->h, 0), len);
}

int grib_accessor_ieeefloat_t::pack_double(const double* val, size_t* len)
{
    int ret            = 0;
    unsigned long i    = 0;
    unsigned long rlen = (unsigned long)*len;
    size_t buflen      = 0;
    unsigned char* buf = NULL;
    long off           = 0;

    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it packs at least 1 value", name_);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if (rlen == 1) {
        off = offset_ * 8;
        ret = grib_encode_unsigned_long(grib_handle_of_accessor(this)->buffer->data, grib_ieee_to_long(val[0]), &off, 32);
        if (*len > 1)
            grib_context_log(context_, GRIB_LOG_WARNING, "ieeefloat: Trying to pack %zu values in a scalar %s, packing first value",
                             *len, name_);
        if (ret == GRIB_SUCCESS)
            len[0] = 1;
        return ret;
    }

    buflen = rlen * 4;

    buf = (unsigned char*)grib_context_malloc(context_, buflen);

    for (i = 0; i < rlen; i++) {
        grib_encode_unsigned_longb(buf, grib_ieee_to_long(val[i]), &off, 32);
    }
    ret = grib_set_long_internal(grib_handle_of_accessor(this), arg_->get_name(parent_->h, 0), rlen);

    if (ret == GRIB_SUCCESS)
        grib_buffer_replace(this, buf, buflen, 1, 1);
    else
        *len = 0;

    grib_context_free(context_, buf);

    return ret;
}

template <typename T>
static int unpack(grib_accessor* a, T* val, size_t* len)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");

    long rlen         = 0;
    int err           = 0;
    long i            = 0;
    long bitp         = a->offset_ * 8;
    grib_handle* hand = grib_handle_of_accessor(a);

    err = a->value_count(&rlen);
    if (err)
        return err;

    if (*len < (size_t)rlen) {
        grib_context_log(a->context_, GRIB_LOG_ERROR, "Wrong size (%zu) for %s, it contains %ld values", *len, a->name_, rlen);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }

    for (i = 0; i < rlen; i++)
        val[i] = (T)grib_long_to_ieee(grib_decode_unsigned_long(hand->buffer->data, &bitp, 32));

    *len = rlen;
    return GRIB_SUCCESS;
}

int grib_accessor_ieeefloat_t::unpack_double(double* val, size_t* len)
{
    return unpack<double>(this, val, len);
}

int grib_accessor_ieeefloat_t::unpack_float(float* val, size_t* len)
{
    return unpack<float>(this, val, len);
}

void grib_accessor_ieeefloat_t::update_size(size_t s)
{
    length_ = (long)s;
    ECCODES_ASSERT(length_ >= 0);
}

int grib_accessor_ieeefloat_t::nearest_smaller_value(double val, double* nearest)
{
    return grib_nearest_smaller_ieee_float(val, nearest);
}
