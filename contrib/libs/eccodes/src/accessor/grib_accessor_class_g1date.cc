/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1date.h"

grib_accessor_g1date_t _grib_accessor_g1date{};
grib_accessor* grib_accessor_g1date = &_grib_accessor_g1date;

void grib_accessor_g1date_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    century_ = c->get_name(hand, n++);
    year_    = c->get_name(hand, n++);
    month_   = c->get_name(hand, n++);
    day_     = c->get_name(hand, n++);
}

int grib_accessor_g1date_t::unpack_long(long* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);

    int ret   = 0;
    long year = 0, century = 0, month = 0, day = 0;

    if ((ret = grib_get_long_internal(hand, century_, &century)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, day_, &day)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, month_, &month)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, year_, &year)) != GRIB_SUCCESS)
        return ret;

    if (*len < 1)
        return GRIB_WRONG_ARRAY_SIZE;

    *val = ((century - 1) * 100 + year) * 10000 + month * 100 + day;

    if (year == 255 && day == 255 && month >= 1 && month <= 12) {
        *val = month;
    }

    if (year == 255 && day != 255 && month >= 1 && month <= 12) {
        *val = month * 100 + day;
    }

    return GRIB_SUCCESS;
}

int grib_accessor_g1date_t::pack_long(const long* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);

    int ret   = 0;
    long v    = val[0];
    long year = 0, century = 0, month = 0, day = 0;

    if (*len != 1)
        return GRIB_WRONG_ARRAY_SIZE;

    long d = grib_julian_to_date(grib_date_to_julian(v));
    if (v != d) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_g1date_t: pack_long invalid date %ld, changed to %ld", v, d);
        return GRIB_ENCODING_ERROR;
    }

    century = v / 1000000;
    v %= 1000000;
    year = v / 10000;
    v %= 10000;
    month = v / 100;
    v %= 100;
    day = v;

    if (year == 0)
        year = 100;
    else
        century++;

    if ((ret = grib_set_long_internal(hand, century_, century)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(hand, day_, day)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(hand, month_, month)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(hand, year_, year)) != GRIB_SUCCESS)
        return ret;

    return GRIB_SUCCESS;
}

static const char* months[] = {
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
};

int grib_accessor_g1date_t::unpack_string(char* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);

    int ret = 0;
    char tmp[1024];
    long year = 0, century = 0, month = 0, day = 0;

    if ((ret = grib_get_long_internal(hand, century_, &century)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, day_, &day)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, month_, &month)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, year_, &year)) != GRIB_SUCCESS)
        return ret;

    if (*len < 1)
        return GRIB_WRONG_ARRAY_SIZE;

    if (year == 255 && day == 255 && month >= 1 && month <= 12) {
        strcpy(tmp, months[month - 1]);
    }
    else if (year == 255 && month >= 1 && month <= 12) {
        snprintf(tmp, sizeof(tmp), "%s-%02ld", months[month - 1], day);
    }
    else {
        long x = ((century - 1) * 100 + year) * 10000 + month * 100 + day;
        snprintf(tmp, sizeof(tmp), "%ld", x);
    }

    size_t l = strlen(tmp) + 1;
    if (*len < l) {
        *len = l;
        return GRIB_BUFFER_TOO_SMALL;
    }

    *len = l;
    strcpy(val, tmp);

    return GRIB_SUCCESS;
}

int grib_accessor_g1date_t::value_count(long* count)
{
    *count = 1;
    return 0;
}
