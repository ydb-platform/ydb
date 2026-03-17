/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_julian_day.h"

grib_accessor_julian_day_t _grib_accessor_julian_day{};
grib_accessor* grib_accessor_julian_day = &_grib_accessor_julian_day;

void grib_accessor_julian_day_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n = 0;
    grib_handle* h  = grib_handle_of_accessor(this);

    date_   = c->get_name(h, n++);
    hour_   = c->get_name(h, n++);
    minute_ = c->get_name(h, n++);
    second_ = c->get_name(h, n++);

    length_ = 0;
}

void grib_accessor_julian_day_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

int grib_accessor_julian_day_t::pack_long(const long* val, size_t* len)
{
    const double v = *val;
    return pack_double(&v, len);
}

int grib_accessor_julian_day_t::pack_double(const double* val, size_t* len)
{
    int ret = GRIB_SUCCESS;
    long hour = 0, minute = 0, second = 0;
    long year = 0, month = 0, day = 0;
    grib_handle* h  = grib_handle_of_accessor(this);

    ret = grib_julian_to_datetime(*val, &year, &month, &day, &hour, &minute, &second);
    if (ret != 0)
        return ret;

    long date = year * 10000 + month * 100 + day;

    ret = grib_set_long_internal(h, date_, date);
    if (ret != 0)
        return ret;
    ret = grib_set_long_internal(h, hour_, hour);
    if (ret != 0)
        return ret;
    ret = grib_set_long_internal(h, minute_, minute);
    if (ret != 0)
        return ret;
    ret = grib_set_long_internal(h, second_, second);

    return ret;
}

int grib_accessor_julian_day_t::unpack_long(long* val, size_t* len)
{
    int ret  = 0;
    double v = 0;

    ret  = unpack_double(&v, len);
    *val = (long)v;

    return ret;
}

int grib_accessor_julian_day_t::unpack_double(double* val, size_t* len)
{
    int ret = 0;
    long date, hour, minute, second;
    long year, month, day;
    grib_handle* h  = grib_handle_of_accessor(this);

    ret = grib_get_long_internal(h, date_, &date);
    if (ret != GRIB_SUCCESS)
        return ret;
    ret = grib_get_long_internal(h, hour_, &hour);
    if (ret != GRIB_SUCCESS)
        return ret;
    ret = grib_get_long_internal(h, minute_, &minute);
    if (ret != GRIB_SUCCESS)
        return ret;
    ret = grib_get_long_internal(h, second_, &second);
    if (ret != GRIB_SUCCESS)
        return ret;

    year = date / 10000;
    date %= 10000;
    month = date / 100;
    date %= 100;
    day = date;

    ret = grib_datetime_to_julian(year, month, day, hour, minute, second, val);

    return ret;
}
