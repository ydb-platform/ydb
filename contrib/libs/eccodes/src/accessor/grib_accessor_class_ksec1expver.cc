/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_ksec1expver.h"

grib_accessor_ksec1expver_t _grib_accessor_ksec1expver{};
grib_accessor* grib_accessor_ksec1expver = &_grib_accessor_ksec1expver;

void grib_accessor_ksec1expver_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_ascii_t::init(len, arg);
    length_ = len;
    ECCODES_ASSERT(length_ >= 0);
}

int grib_accessor_ksec1expver_t::unpack_long(long* val, size_t* len)
{
    long value  = 0;
    long pos    = offset_ * 8;
    char* intc  = NULL;
    char* pTemp = NULL;
    char expver[5];
    char refexpver[5];
    size_t llen = length_ + 1;
    ECCODES_ASSERT(length_ == 4);

    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s it contains %d values ", name_, 1);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }
    value = grib_decode_unsigned_long(grib_handle_of_accessor(this)->buffer->data, &pos, length_ * 8);

    unpack_string(refexpver, &llen);
    /* test for endian */
    intc  = (char*)&value;
    pTemp = intc;

    expver[0] = *pTemp++;
    expver[1] = *pTemp++;
    expver[2] = *pTemp++;
    expver[3] = *pTemp++;
    expver[4] = 0;

    //     expver[0] = intc[0];
    //     expver[1] = intc[1];
    //     expver[2] = intc[2];
    //     expver[3] = intc[3];
    //     expver[4] = 0;

    /* if there is a difference, have to reverse*/
    if (strcmp(refexpver, expver)) {
        intc[0] = expver[3];
        intc[1] = expver[2];
        intc[2] = expver[1];
        intc[3] = expver[0];
    }

    *val = value;
    *len = 1;
    return GRIB_SUCCESS;
}

int grib_accessor_ksec1expver_t::pack_string(const char* val, size_t* len)
{
    int i = 0;
    if (len[0] != 4) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong length for %s. It has to be 4", name_);
        return GRIB_INVALID_KEY_VALUE;
    }
    if (len[0] > (length_) + 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "pack_string: Wrong size (%lu) for %s, it contains %ld values",
                         len[0], name_, length_ + 1);
        len[0] = 0;
        return GRIB_BUFFER_TOO_SMALL;
    }

    for (i = 0; i < length_; i++)
        grib_handle_of_accessor(this)->buffer->data[offset_ + i] = val[i];

    return GRIB_SUCCESS;
}

int grib_accessor_ksec1expver_t::pack_long(const long* val, size_t* len)
{
    char sval[5] = {0,};
    size_t slen = 4;
    snprintf(sval, sizeof(sval), "%04d", (int)(*val));
    return pack_string(sval, &slen);
}
