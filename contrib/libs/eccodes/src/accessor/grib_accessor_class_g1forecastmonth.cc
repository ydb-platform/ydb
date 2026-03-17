/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1forecastmonth.h"

grib_accessor_g1forecastmonth_t _grib_accessor_g1forecastmonth{};
grib_accessor* grib_accessor_g1forecastmonth = &_grib_accessor_g1forecastmonth;

void grib_accessor_g1forecastmonth_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    grib_handle* h  = grib_handle_of_accessor(this);
    int n           = 0;
    const int count = c->get_count();
    if (count == 6) { /* GRIB1 case -- this needs to be refactored */
        verification_yearmonth_ = c->get_name(h, n++);
        base_date_              = c->get_name(h, n++);
        day_                    = c->get_name(h, n++);
        hour_                   = c->get_name(h, n++);
        fcmonth_                = c->get_name(h, n++);
        check_                  = c->get_name(h, n++);
    }
}

void grib_accessor_g1forecastmonth_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_long(this, NULL);
}

static int calculate_fcmonth(grib_accessor* a, long verification_yearmonth, long base_date, long day, long hour, long* result)
{
    long base_yearmonth = 0;
    long vyear          = 0;
    long vmonth         = 0;
    long byear          = 0;
    long bmonth         = 0;
    long fcmonth        = 0;

    base_yearmonth = base_date / 100;

    vyear  = verification_yearmonth / 100;
    vmonth = verification_yearmonth % 100;
    byear  = base_yearmonth / 100;
    bmonth = base_yearmonth % 100;

    fcmonth = (vyear - byear) * 12 + (vmonth - bmonth);
    if (day == 1 && hour == 0)
        fcmonth++;

    *result = fcmonth;
    return GRIB_SUCCESS;
}

static int unpack_long_edition2(grib_accessor* a, long* val, size_t* len)
{
    int err                               = 0;
    grib_handle* h                        = grib_handle_of_accessor(a);
    long dataDate, verification_yearmonth;
    long year, month, day, hour, minute, second;
    long year2, month2, day2, hour2, minute2, second2;
    long forecastTime, indicatorOfUnitOfTimeRange;
    double jul_base, jul2, dstep;

    if ((err = grib_get_long(h, "year", &year)) != GRIB_SUCCESS) return err;
    if ((err = grib_get_long(h, "month", &month)) != GRIB_SUCCESS) return err;
    if ((err = grib_get_long(h, "day", &day)) != GRIB_SUCCESS) return err;
    if ((err = grib_get_long(h, "hour", &hour)) != GRIB_SUCCESS) return err;
    if ((err = grib_get_long(h, "minute", &minute)) != GRIB_SUCCESS) return err;
    if ((err = grib_get_long(h, "second", &second)) != GRIB_SUCCESS) return err;

    if ((err = grib_get_long_internal(h, "dataDate", &dataDate)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(h, "forecastTime", &forecastTime)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, "indicatorOfUnitOfTimeRange", &indicatorOfUnitOfTimeRange)) != GRIB_SUCCESS)
        return err;
    if (indicatorOfUnitOfTimeRange != 1) { /* must be hour */
        grib_context_log(a->context_, GRIB_LOG_ERROR, "indicatorOfUnitOfTimeRange must be 1 (hour)");
        return GRIB_DECODING_ERROR;
    }

    if ((err = grib_datetime_to_julian(year, month, day, hour, minute, second, &jul_base)) != GRIB_SUCCESS)
        return err;

    dstep = (((double)forecastTime) * 3600) / 86400; /* as a fraction of a day */
    jul2  = jul_base + dstep;

    if ((err = grib_julian_to_datetime(jul2, &year2, &month2, &day2, &hour2, &minute2, &second2)) != GRIB_SUCCESS)
        return err;

    verification_yearmonth = year2 * 100 + month2;
    if ((err = calculate_fcmonth(a, verification_yearmonth, dataDate, day, hour, val)) != GRIB_SUCCESS)
        return err;

    return GRIB_SUCCESS;
}

int grib_accessor_g1forecastmonth_t::unpack_long_edition1(long* val, size_t* len)
{
    int err                               = 0;

    long verification_yearmonth = 0;
    long base_date              = 0;
    long day                    = 0;
    long hour                   = 0;
    long gribForecastMonth      = 0;
    long check                  = 0;
    long fcmonth                = 0;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this),
                                      verification_yearmonth_, &verification_yearmonth)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), base_date_, &base_date)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), day_, &day)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), hour_, &hour)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), fcmonth_, &gribForecastMonth)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), check_, &check)) != GRIB_SUCCESS)
        return err;

    if ((err = calculate_fcmonth(this, verification_yearmonth, base_date, day, hour, val)) != GRIB_SUCCESS)
        return err;

    /* Verification - compare gribForecastMonth with fcmonth */
    fcmonth = *val;
    if (gribForecastMonth != 0 && gribForecastMonth != fcmonth) {
        if (check) {
            grib_context_log(context_, GRIB_LOG_ERROR, "%s=%ld (%s-%s)=%ld", fcmonth_,
                             gribForecastMonth, base_date, verification_yearmonth_, fcmonth);
            ECCODES_ASSERT(gribForecastMonth == fcmonth);
        }
        else {
            *val = gribForecastMonth;
            return GRIB_SUCCESS;
        }
    }

    return GRIB_SUCCESS;
}

int grib_accessor_g1forecastmonth_t::unpack_long(long* val, size_t* len)
{
    int err           = 0;
    grib_handle* hand = grib_handle_of_accessor(this);
    long edition      = 0;

    if ((err = grib_get_long(hand, "edition", &edition)) != GRIB_SUCCESS)
        return err;

    if (edition == 1)
        return unpack_long_edition1(val, len);
    if (edition == 2)
        return unpack_long_edition2(this, val, len);

    return GRIB_UNSUPPORTED_EDITION;
}

/* TODO: Check for a valid date */
int grib_accessor_g1forecastmonth_t::pack_long(const long* val, size_t* len)
{
    return grib_set_long_internal(grib_handle_of_accessor(this), fcmonth_, *val);
}
