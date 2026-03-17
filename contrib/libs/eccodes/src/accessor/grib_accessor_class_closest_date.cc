/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_closest_date.h"
#include <float.h>

grib_accessor_closest_date_t _grib_accessor_closest_date{};
grib_accessor* grib_accessor_closest_date = &_grib_accessor_closest_date;

void grib_accessor_closest_date_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    grib_handle* h = grib_handle_of_accessor(this);
    int n          = 0;

    dateLocal_    = c->get_name(h, n++);
    timeLocal_    = c->get_name(h, n++);
    numForecasts_ = c->get_name(h, n++);
    year_         = c->get_name(h, n++);
    month_        = c->get_name(h, n++);
    day_          = c->get_name(h, n++);
    hour_         = c->get_name(h, n++);
    minute_       = c->get_name(h, n++);
    second_       = c->get_name(h, n++);

    length_ = 0;
}

void grib_accessor_closest_date_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

int grib_accessor_closest_date_t::unpack_long(long* val, size_t* len)
{
    int ret  = 0;
    double v = 0;

    ret  = unpack_double(&v, len);
    *val = (long)v;

    return ret;
}

/* Sets val to the 'index' of the closes date */
int grib_accessor_closest_date_t::unpack_double(double* val, size_t* len)
{
    int err            = 0;
    long num_forecasts = 0; /* numberOfForecastsUsedInLocalTime */
    /* These relate to the date and time in Section 1 */
    long ymdLocal, hmsLocal, yearLocal, monthLocal, dayLocal, hourLocal, minuteLocal, secondLocal;
    double jLocal  = 0;
    double minDiff = DBL_MAX;
    size_t i       = 0;
    size_t size    = 0; /* number of elements in the array keys - should be = numberOfForecastsUsedInLocalTime */

    /* These relate to the forecast dates and times in Section 4 */
    long *yearArray, *monthArray, *dayArray, *hourArray, *minuteArray, *secondArray;

    grib_handle* h        = grib_handle_of_accessor(this);
    const grib_context* c = context_;
    *val                  = -1; /* initialise to an invalid index */

    if ((err = grib_get_long_internal(h, numForecasts_, &num_forecasts)) != GRIB_SUCCESS) return err;
    ECCODES_ASSERT(num_forecasts > 1);

    if ((err = grib_get_long(h, dateLocal_, &ymdLocal)) != GRIB_SUCCESS) return err;
    yearLocal = ymdLocal / 10000;
    ymdLocal %= 10000;
    monthLocal = ymdLocal / 100;
    ymdLocal %= 100;
    dayLocal = ymdLocal;

    if ((err = grib_get_long(h, timeLocal_, &hmsLocal)) != GRIB_SUCCESS) return err;
    hourLocal = hmsLocal / 100;
    hmsLocal %= 100;
    minuteLocal = hmsLocal / 100;
    hmsLocal %= 100;
    secondLocal = hmsLocal;

    if ((err = grib_get_size(h, year_, &size)) != GRIB_SUCCESS) return err;
    ECCODES_ASSERT(size == (size_t)num_forecasts);
    yearArray = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if ((err = grib_get_long_array_internal(h, year_, yearArray, &size)) != GRIB_SUCCESS) return err;

    if ((err = grib_get_size(h, month_, &size)) != GRIB_SUCCESS) return err;
    ECCODES_ASSERT(size == (size_t)num_forecasts);
    monthArray = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if ((err = grib_get_long_array_internal(h, month_, monthArray, &size)) != GRIB_SUCCESS) return err;

    if ((err = grib_get_size(h, day_, &size)) != GRIB_SUCCESS) return err;
    ECCODES_ASSERT(size == (size_t)num_forecasts);
    dayArray = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if ((err = grib_get_long_array_internal(h, day_, dayArray, &size)) != GRIB_SUCCESS) return err;

    if ((err = grib_get_size(h, hour_, &size)) != GRIB_SUCCESS) return err;
    ECCODES_ASSERT(size == (size_t)num_forecasts);
    hourArray = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if ((err = grib_get_long_array_internal(h, hour_, hourArray, &size)) != GRIB_SUCCESS) return err;

    if ((err = grib_get_size(h, minute_, &size)) != GRIB_SUCCESS) return err;
    ECCODES_ASSERT(size == (size_t)num_forecasts);
    minuteArray = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if ((err = grib_get_long_array_internal(h, minute_, minuteArray, &size)) != GRIB_SUCCESS) return err;

    if ((err = grib_get_size(h, second_, &size)) != GRIB_SUCCESS) return err;
    ECCODES_ASSERT(size == (size_t)num_forecasts);
    secondArray = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if ((err = grib_get_long_array_internal(h, second_, secondArray, &size)) != GRIB_SUCCESS) return err;

    grib_datetime_to_julian(yearLocal, monthLocal, dayLocal, hourLocal, minuteLocal, secondLocal, &jLocal);
    for (i = 0; i < size; ++i) {
        double jval = 0, diff = 0;
        grib_datetime_to_julian(yearArray[i], monthArray[i], dayArray[i],
                                hourArray[i], minuteArray[i], secondArray[i], &jval);
        diff = jLocal - jval;
        if (diff >= 0 && diff < minDiff) {
            minDiff = diff;
            *val    = i;
        }
    }
    if (*val == -1) {
        grib_context_log(c, GRIB_LOG_ERROR, "Failed to find a date/time amongst forecasts used in local time");
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

cleanup:
    grib_context_free(c, yearArray);
    grib_context_free(c, monthArray);
    grib_context_free(c, dayArray);
    grib_context_free(c, hourArray);
    grib_context_free(c, minuteArray);
    grib_context_free(c, secondArray);

    return err;
}
