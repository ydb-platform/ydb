/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_validity_time.h"

grib_accessor_validity_time_t _grib_accessor_validity_time{};
grib_accessor* grib_accessor_validity_time = &_grib_accessor_validity_time;

void grib_accessor_validity_time_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    date_      = c->get_name(hand, n++);
    time_      = c->get_name(hand, n++);
    step_      = c->get_name(hand, n++);
    stepUnits_ = c->get_name(hand, n++);
    hours_     = c->get_name(hand, n++);
    minutes_   = c->get_name(hand, n++);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_validity_time_t::unpack_long(long* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    int ret           = 0;
    long date         = 0;
    long time         = 0;
    long step         = 0;
    long stepUnits    = 0;
    long hours = 0, minutes = 0, step_mins = 0, tmp, tmp_hrs, tmp_mins;

    if (hours_) {
        if ((ret = grib_get_long_internal(hand, hours_, &hours)) != GRIB_SUCCESS)
            return ret;
        if ((ret = grib_get_long_internal(hand, minutes_, &minutes)) != GRIB_SUCCESS)
            return ret;
        *val = hours * 100 + minutes;
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

    /* Seconds will always be zero. So convert to minutes */
    if (stepUnits_) {
        if ((ret = grib_get_long_internal(hand, stepUnits_, &stepUnits)) != GRIB_SUCCESS)
            return ret;
        step_mins = convert_to_minutes(step, stepUnits);
    }

    minutes  = time % 100;
    hours    = time / 100;
    tmp      = minutes + step_mins; /* add the step to our minutes */
    tmp_hrs  = tmp / 60;            /* how many hours and mins is that? */
    tmp_mins = tmp % 60;
    hours += tmp_hrs; /* increment hours */
    if (hours > 0) {
        hours = hours % 24; /* wrap round if >= 24 */
    }
    else {
        /* GRIB-29: Negative forecast time */
        while (hours < 0) {
            hours += 24;
        }
    }
    time = hours * 100 + tmp_mins;

    if (*len < 1)
        return GRIB_ARRAY_TOO_SMALL;

    *val = time;

    return GRIB_SUCCESS;
}

int grib_accessor_validity_time_t::unpack_string(char* val, size_t* len)
{
    int err      = 0;
    long v       = 0;
    size_t lsize = 1, lmin = 5;

    err = unpack_long(&v, &lsize);
    if (err) return err;

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
