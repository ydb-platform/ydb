/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_budgdate.h"

grib_accessor_budgdate_t _grib_accessor_budgdate{};
grib_accessor* grib_accessor_budgdate = &_grib_accessor_budgdate;

void grib_accessor_budgdate_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n = 0;

    year_  = c->get_name(grib_handle_of_accessor(this), n++);
    month_ = c->get_name(grib_handle_of_accessor(this), n++);
    day_   = c->get_name(grib_handle_of_accessor(this), n++);
}

int grib_accessor_budgdate_t::unpack_long(long* val, size_t* len)
{
    int ret = 0;

    long year  = 0;
    long month = 0;
    long day   = 0;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), day_, &day)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), month_, &month)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), year_, &year)) != GRIB_SUCCESS)
        return ret;

    if (*len < 1)
        return GRIB_WRONG_ARRAY_SIZE;

    val[0] = (1900 + year) * 10000 + month * 100 + day;

    return ret;
}

/* TODO: Check for a valid date */
int grib_accessor_budgdate_t::pack_long(const long* val, size_t* len)
{
    int ret = 0;
    long v  = val[0];

    long year  = 0;
    long month = 0;
    long day   = 0;

    if (*len != 1)
        return GRIB_WRONG_ARRAY_SIZE;

    year = v / 10000;
    v %= 10000;
    month = v / 100;
    v %= 100;
    day = v;

    year -= 1900;

    ECCODES_ASSERT(year < 255);

    if ((ret = grib_set_long_internal(grib_handle_of_accessor(this), day_, day)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(grib_handle_of_accessor(this), month_, month)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(grib_handle_of_accessor(this), year_, year)) != GRIB_SUCCESS)
        return ret;

    return ret;
}
