/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_rdbtime_guess_date.h"

grib_accessor_rdbtime_guess_date_t _grib_accessor_rdbtime_guess_date{};
grib_accessor* grib_accessor_rdbtime_guess_date = &_grib_accessor_rdbtime_guess_date;

void grib_accessor_rdbtime_guess_date_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n = 0;

    typicalYear_  = c->get_name(grib_handle_of_accessor(this), n++);
    typicalMonth_ = c->get_name(grib_handle_of_accessor(this), n++);
    typicalDay_   = c->get_name(grib_handle_of_accessor(this), n++);
    rdbDay_       = c->get_name(grib_handle_of_accessor(this), n++);
    yearOrMonth_  = c->get_long(grib_handle_of_accessor(this), n++);

    /* flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY; */
}

int grib_accessor_rdbtime_guess_date_t::unpack_long(long* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);
    long typicalYear, typicalMonth, typicalDay, rdbDay;
    long rdbYear, rdbMonth;

    int ret = grib_get_long(h, typicalYear_, &typicalYear);
    if (ret)
        return ret;
    ret = grib_get_long(h, typicalMonth_, &typicalMonth);
    if (ret)
        return ret;
    ret = grib_get_long(h, typicalDay_, &typicalDay);
    if (ret)
        return ret;
    ret = grib_get_long(h, rdbDay_, &rdbDay);
    if (ret)
        return ret;

    if (rdbDay < typicalDay) {
        if (typicalDay == 31 && typicalMonth == 12) {
            rdbYear  = typicalYear + 1;
            rdbMonth = 1;
        }
        else {
            rdbYear  = typicalYear;
            rdbMonth = typicalMonth + 1;
        }
    }
    else {
        rdbYear  = typicalYear;
        rdbMonth = typicalMonth;
    }

    *val = yearOrMonth_ == 1 ? rdbYear : rdbMonth;
    *len = 1;

    return GRIB_SUCCESS;
}

int grib_accessor_rdbtime_guess_date_t::pack_long(const long* v, size_t* len)
{
    /* do nothing*/
    return GRIB_SUCCESS;
}
