/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_validity_date.h"

grib_accessor_validity_date_t _grib_accessor_validity_date{};
grib_accessor* grib_accessor_validity_date = &_grib_accessor_validity_date;

void grib_accessor_validity_date_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    date_      = c->get_name(hand, n++);
    time_      = c->get_name(hand, n++);
    step_      = c->get_name(hand, n++);
    stepUnits_ = c->get_name(hand, n++);
    year_      = c->get_name(hand, n++);
    month_     = c->get_name(hand, n++);
    day_       = c->get_name(hand, n++);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_validity_date_t::unpack_long(long* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    int ret           = 0;
    long date         = 0;
    long time         = 0;
    long step         = 0;
    long stepUnits    = 0;
    long hours = 0, minutes = 0, step_mins = 0, tmp, tmp_hrs;

    if (year_) {
        long year, month, day;
        if ((ret = grib_get_long_internal(hand, year_, &year)) != GRIB_SUCCESS)
            return ret;
        if ((ret = grib_get_long_internal(hand, month_, &month)) != GRIB_SUCCESS)
            return ret;
        if ((ret = grib_get_long_internal(hand, day_, &day)) != GRIB_SUCCESS)
            return ret;
        *val = year * 10000 + month * 100 + day;
        return GRIB_SUCCESS;
    }
    if ((ret = grib_get_long_internal(hand, date_, &date)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(hand, time_, &time)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long(hand, step_, &step)) != GRIB_SUCCESS) {
        if ((ret = grib_get_long_internal(hand, "endStep", &step)) != GRIB_SUCCESS) {
            return ret; /* See ECC-817 */
        }
    }

    if (stepUnits_) {
        if ((ret = grib_get_long_internal(hand, stepUnits_, &stepUnits)) != GRIB_SUCCESS)
            return ret;
        step_mins = convert_to_minutes(step, stepUnits);
    }

    minutes = time % 100;
    hours   = time / 100;
    tmp     = minutes + step_mins; /* add the step to our minutes */
    tmp_hrs = tmp / 60;            /* how many hours and mins is that? */
    hours += tmp_hrs;              /* increment hours */

    date = grib_date_to_julian(date);
    /* does the new 'hours' exceed 24? if so increment julian */
    while (hours >= 24) {
        date++;
        hours -= 24;
    }
    /* GRIB-29: Negative forecast time */
    while (hours < 0) {
        date--;
        hours += 24;
    }

    if (*len < 1)
        return GRIB_ARRAY_TOO_SMALL;

    *val = grib_julian_to_date(date);

    return GRIB_SUCCESS;
}
