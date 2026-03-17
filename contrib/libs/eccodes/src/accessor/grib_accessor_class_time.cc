/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_time.h"

grib_accessor_time_t _grib_accessor_time{};
grib_accessor* grib_accessor_time = &_grib_accessor_time;

void grib_accessor_time_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    hour_   = c->get_name(hand, n++);
    minute_ = c->get_name(hand, n++);
    second_ = c->get_name(hand, n++);
}

int grib_accessor_time_t::unpack_long(long* val, size_t* len)
{
    int ret   = 0;
    long hour = 0, minute = 0, second = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    if ((ret = grib_get_long_internal(hand, hour_, &hour)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, minute_, &minute)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, second_, &second)) != GRIB_SUCCESS)
        return ret;

    /* We ignore the 'seconds' in our time calculation! */
    if (second != 0) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Key %s (%s): Truncating time: non-zero seconds(%ld) ignored", name_, __func__, second);
    }

    if (*len < 1)
        return GRIB_WRONG_ARRAY_SIZE;

    *val = hour * 100 + minute;

    if (hour == 255) {
        *val = 12 * 100;
    }
    if (hour != 255 && minute == 255) {
        *val = hour * 100;
    }
    return GRIB_SUCCESS;
}

int grib_accessor_time_t::pack_long(const long* val, size_t* len)
{
    int ret           = 0;
    long v            = val[0];
    grib_handle* hand = grib_handle_of_accessor(this);
    long hour = 0, minute = 0, second = 0;

    if (*len != 1)
        return GRIB_WRONG_ARRAY_SIZE;

    if (!is_time_valid(v)) {
        // ECC-1777: For now just a warning. Will later change to an error
        fprintf(stderr, "ECCODES WARNING :  %s:%s: Time is not valid! hour=%ld min=%ld sec=%ld\n",
                class_name_, __func__, hour, minute, second);
        // return GRIB_ENCODING_ERROR;
    }

    hour   = v / 100;
    minute = v % 100;
    second = 0; /* We ignore the 'seconds' in our time calculation! */

    if ((ret = grib_set_long_internal(hand, hour_, hour)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(hand, minute_, minute)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(hand, second_, second)) != GRIB_SUCCESS)
        return ret;

    return GRIB_SUCCESS;
}

int grib_accessor_time_t::unpack_string(char* val, size_t* len)
{
    long v       = 0;
    size_t lsize = 1, lmin = 5;

    unpack_long(&v, &lsize);

    if (*len < lmin) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, lmin, *len);
        *len = lmin;
        return GRIB_BUFFER_TOO_SMALL;
    }

    snprintf(val, 64, "%04ld", v);

    len[0] = lmin;
    return GRIB_SUCCESS;
}
