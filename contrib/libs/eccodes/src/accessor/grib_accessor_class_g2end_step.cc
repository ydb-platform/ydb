/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2end_step.h"
#include "step.h"
#include "step_utilities.h"
#include <stdexcept>

grib_accessor_g2end_step_t _grib_accessor_g2end_step{};
grib_accessor* grib_accessor_g2end_step = &_grib_accessor_g2end_step;

void grib_accessor_g2end_step_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n          = 0;
    grib_handle* h = grib_handle_of_accessor(this);

    start_step_value_ = c->get_name(h, n++);
    step_units_       = c->get_name(h, n++);

    year_   = c->get_name(h, n++);
    month_  = c->get_name(h, n++);
    day_    = c->get_name(h, n++);
    hour_   = c->get_name(h, n++);
    minute_ = c->get_name(h, n++);
    second_ = c->get_name(h, n++);

    year_of_end_of_interval_   = c->get_name(h, n++);
    month_of_end_of_interval_  = c->get_name(h, n++);
    day_of_end_of_interval_    = c->get_name(h, n++);
    hour_of_end_of_interval_   = c->get_name(h, n++);
    minute_of_end_of_interval_ = c->get_name(h, n++);
    second_of_end_of_interval_ = c->get_name(h, n++);

    time_range_unit_     = c->get_name(h, n++);
    time_range_value_    = c->get_name(h, n++);
    typeOfTimeIncrement_ = c->get_name(h, n++);
    numberOfTimeRanges_  = c->get_name(h, n++);
}

void grib_accessor_g2end_step_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_double(this, NULL);
}

// See GRIB-488
static bool is_special_expver(const grib_handle* h)
{
    char strMarsExpVer[50] = {0,};
    char strMarsClass[50] = {0,};
    size_t slen = 50;
    int ret = grib_get_string(h, "mars.class", strMarsClass, &slen);
    if (ret == GRIB_SUCCESS && STR_EQUAL(strMarsClass, "em")) {
        // em = ERA-CLIM model integration for the 20th-century (ERA-20CM)
        slen = 50;
        ret  = grib_get_string(h, "experimentVersionNumber", strMarsExpVer, &slen);
        if (ret == GRIB_SUCCESS && STR_EQUAL(strMarsExpVer, "1605")) {
            return true;  // Special case of expVer 1605 in class "em"
        }
    }

    return false;
}

static int convert_time_range_long_(
    grib_handle* h,
    long stepUnits,                   /* step_units */
    long indicatorOfUnitForTimeRange, /* time_range_unit */
    long* lengthOfTimeRange           /* time_range_value */
)
{
    ECCODES_ASSERT(lengthOfTimeRange != NULL);

    if (indicatorOfUnitForTimeRange != stepUnits) {
        eccodes::Step time_range{ *lengthOfTimeRange, indicatorOfUnitForTimeRange };
        time_range.set_unit(eccodes::Unit{ stepUnits });
        if (time_range.value<long>() != time_range.value<double>()) {
            return GRIB_DECODING_ERROR;
        }
        *lengthOfTimeRange = time_range.value<long>();
    }

    return GRIB_SUCCESS;
}

int grib_accessor_g2end_step_t::unpack_one_time_range_long_(long* val, size_t* len)
{
    int err = 0;
    long start_step_value;
    long step_units;
    long time_range_unit;
    long time_range_value, typeOfTimeIncrement;
    int add_time_range = 1; /* whether we add lengthOfTimeRange */

    grib_handle* h = grib_handle_of_accessor(this);

    if ((err = grib_get_long_internal(h, start_step_value_, &start_step_value)))
        return err;
    if ((err = grib_get_long_internal(h, step_units_, &step_units)))
        return err;
    if ((err = grib_get_long_internal(h, time_range_unit_, &time_range_unit)))
        return err;
    if ((err = grib_get_long_internal(h, time_range_value_, &time_range_value)))
        return err;
    if ((err = grib_get_long_internal(h, typeOfTimeIncrement_, &typeOfTimeIncrement)))
        return err;

    err = convert_time_range_long_(h, step_units, time_range_unit, &time_range_value);
    if (err != GRIB_SUCCESS)
        return err;

    if (typeOfTimeIncrement == 1) {
        /* See GRIB-488 */
        /* Note: For this case, lengthOfTimeRange is not related to step and should not be used to calculate step */
        add_time_range = 0;
        if (is_special_expver(h)) {
            add_time_range = 1;
        }
    }
    if (add_time_range) {
        *val = start_step_value + time_range_value;
        if ((err = grib_set_long_internal(h, "endStepUnit", step_units)) != GRIB_SUCCESS)
            return err;
    }
    else {
        *val = start_step_value;
        if ((err = grib_set_long_internal(h, "endStepUnit", step_units)) != GRIB_SUCCESS)
            return err;
    }

    return GRIB_SUCCESS;
}

int grib_accessor_g2end_step_t::unpack_one_time_range_double_(double* val, size_t* len)
{
    int err                          = 0;
    double start_step_value;
    long start_step_unit;
    long step_units;
    long time_range_unit;
    double time_range_value;
    long typeOfTimeIncrement;
    int add_time_range = 1; /* whether we add lengthOfTimeRange */

    grib_handle* h = grib_handle_of_accessor(this);

    if ((err = grib_get_double_internal(h, start_step_value_, &start_step_value)))
        return err;
    if ((err = grib_get_long_internal(h, "startStepUnit", &start_step_unit)))
        return err;
    if ((err = grib_get_long_internal(h, step_units_, &step_units)))
        return err;
    if ((err = grib_get_long_internal(h, time_range_unit_, &time_range_unit)))
        return err;
    if ((err = grib_get_double_internal(h, time_range_value_, &time_range_value)))
        return err;
    if ((err = grib_get_long_internal(h, typeOfTimeIncrement_, &typeOfTimeIncrement)))
        return err;

    eccodes::Step start_step{ start_step_value, start_step_unit };
    eccodes::Step time_range{ time_range_value, time_range_unit };

    if (typeOfTimeIncrement == 1) {
        /* See GRIB-488 */
        /* Note: For this case, lengthOfTimeRange is not related to step and should not be used to calculate step */
        add_time_range = 0;
        if (is_special_expver(h)) {
            add_time_range = 1;
        }
    }
    if (add_time_range) {
        *val = (start_step + time_range).value<double>(eccodes::Unit(step_units));
        if ((err = grib_set_long_internal(h, "endStepUnit", step_units)) != GRIB_SUCCESS)
            return err;
    }
    else {
        *val = start_step.value<double>(eccodes::Unit(start_step_unit));
        if ((err = grib_set_long_internal(h, "endStepUnit", start_step_unit)) != GRIB_SUCCESS)
            return err;
    }

    return GRIB_SUCCESS;
}

#define MAX_NUM_TIME_RANGES 16 /* maximum number of time range specifications */
int grib_accessor_g2end_step_t::unpack_multiple_time_ranges_long_(long* val, size_t* len)
{
    int i = 0, err = 0;
    grib_handle* h          = grib_handle_of_accessor(this);
    long numberOfTimeRanges = 0, step_units = 0, start_step_value = 0;

    size_t count                                      = 0;
    long arr_typeOfTimeIncrement[MAX_NUM_TIME_RANGES] = {
        0,
    };
    long arr_coded_unit[MAX_NUM_TIME_RANGES] = {
        0,
    };
    long arr_coded_time_range[MAX_NUM_TIME_RANGES] = {
        0,
    };

    if ((err = grib_get_long_internal(h, start_step_value_, &start_step_value)))
        return err;
    if ((err = grib_get_long_internal(h, step_units_, &step_units)))
        return err;
    if ((err = grib_get_long_internal(h, numberOfTimeRanges_, &numberOfTimeRanges)))
        return err;
    if (numberOfTimeRanges > MAX_NUM_TIME_RANGES) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "Too many time range specifications!");
        return GRIB_DECODING_ERROR;
    }

    count = numberOfTimeRanges;
    /* Get the arrays for the N time ranges */
    if ((err = grib_get_long_array(h, typeOfTimeIncrement_, arr_typeOfTimeIncrement, &count)))
        return err;
    if ((err = grib_get_long_array(h, time_range_unit_, arr_coded_unit, &count)))
        return err;
    if ((err = grib_get_long_array(h, time_range_value_, arr_coded_time_range, &count)))
        return err;

    /* Look in the array of typeOfTimeIncrements for first entry whose typeOfTimeIncrement == 2 */
    for (i = 0; i < count; i++) {
        if (arr_typeOfTimeIncrement[i] == 2) {
            /* Found the required time range. Get the other two keys from it */
            long the_coded_unit       = arr_coded_unit[i];
            long the_coded_time_range = arr_coded_time_range[i];

            err = convert_time_range_long_(h, step_units, the_coded_unit, &the_coded_time_range);
            if (err != GRIB_SUCCESS)
                return err;

            *val = start_step_value + the_coded_time_range;
            return GRIB_SUCCESS;
        }
    }

    grib_context_log(h->context, GRIB_LOG_ERROR,
                     "Cannot calculate endStep. No time range specification with typeOfTimeIncrement = 2");
    return GRIB_DECODING_ERROR;
}

int grib_accessor_g2end_step_t::unpack_multiple_time_ranges_double_(double* val, size_t* len)
{
    int i = 0, err = 0;
    grib_handle* h          = grib_handle_of_accessor(this);
    long numberOfTimeRanges = 0;
    long step_units         = 0;
    long start_step_value   = 0;
    long start_step_unit    = 0;

    size_t count                                      = 0;
    long arr_typeOfTimeIncrement[MAX_NUM_TIME_RANGES] = {
        0,
    };
    long arr_coded_unit[MAX_NUM_TIME_RANGES] = {
        0,
    };
    long arr_coded_time_range[MAX_NUM_TIME_RANGES] = {
        0,
    };

    if ((err = grib_get_long_internal(h, start_step_value_, &start_step_value)))
        return err;
    if ((err = grib_get_long_internal(h, "startStepUnit", &start_step_unit)))
        return err;

    eccodes::Step start_step{ start_step_value, start_step_unit };

    if ((err = grib_get_long_internal(h, step_units_, &step_units)))
        return err;

    if ((err = grib_get_long_internal(h, numberOfTimeRanges_, &numberOfTimeRanges)))
        return err;
    if (numberOfTimeRanges > MAX_NUM_TIME_RANGES) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "Too many time range specifications!");
        return GRIB_DECODING_ERROR;
    }

    count = numberOfTimeRanges;
    /* Get the arrays for the N time ranges */
    if ((err = grib_get_long_array(h, typeOfTimeIncrement_, arr_typeOfTimeIncrement, &count)))
        return err;
    if ((err = grib_get_long_array(h, time_range_unit_, arr_coded_unit, &count)))
        return err;
    if ((err = grib_get_long_array(h, time_range_value_, arr_coded_time_range, &count)))
        return err;

    /* Look in the array of typeOfTimeIncrements for first entry whose typeOfTimeIncrement == 2 */
    for (i = 0; i < count; i++) {
        if (arr_typeOfTimeIncrement[i] == 2) {
            /* Found the required time range. Get the other two keys from it */
            long the_coded_unit       = arr_coded_unit[i];
            long the_coded_time_range = arr_coded_time_range[i];

            eccodes::Step time_range{ the_coded_time_range, the_coded_unit };
            *val = (start_step + time_range).value<double>(eccodes::Unit(step_units));

            return GRIB_SUCCESS;
        }
    }

    grib_context_log(h->context, GRIB_LOG_ERROR,
                     "Cannot calculate endStep. No time range specification with typeOfTimeIncrement = 2");
    return GRIB_DECODING_ERROR;
}

// For the old implementation of unpack_long, see
//  src/deprecated/grib_accessor_g2end_step.unpack_long.cc
//
int grib_accessor_g2end_step_t::unpack_long(long* val, size_t* len)
{
    grib_handle* h          = grib_handle_of_accessor(this);
    int ret                 = 0;
    long start_step_value   = 0;
    long start_step_unit    = 0;
    long numberOfTimeRanges = 0;

    if ((ret = grib_get_long_internal(h, start_step_value_, &start_step_value)))
        return ret;
    if ((ret = grib_get_long_internal(h, "startStepUnit", &start_step_unit)))
        return ret;

    /* point in time */
    if (year_ == NULL) {
        *val = start_step_value;
        if ((ret = grib_set_long_internal(h, "endStepUnit", start_step_unit)))
            return ret;
        return 0;
    }

    ECCODES_ASSERT(numberOfTimeRanges_);
    if ((ret = grib_get_long_internal(h, numberOfTimeRanges_, &numberOfTimeRanges)))
        return ret;
    ECCODES_ASSERT(numberOfTimeRanges == 1 || numberOfTimeRanges == 2);

    try {
        if (numberOfTimeRanges == 1) {
            ret = unpack_one_time_range_long_(val, len);
        }
        else {
            ret = unpack_multiple_time_ranges_long_(val, len);
        }
    }
    catch (std::exception& e) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "grib_accessor_g2end_step_t::unpack_long: %s", e.what());
        ret = GRIB_DECODING_ERROR;
    }

    return ret;
}

int grib_accessor_g2end_step_t::unpack_double(double* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);
    int ret        = 0;
    long start_step_value;
    long start_step_unit;
    long numberOfTimeRanges;

    if ((ret = grib_get_long_internal(h, start_step_value_, &start_step_value)))
        return ret;
    if ((ret = grib_get_long_internal(h, "startStepUnit", &start_step_unit)))
        return ret;

    /* point in time */
    if (year_ == NULL) {
        *val = start_step_value;
        if ((ret = grib_set_long_internal(h, "endStepUnit", start_step_unit)))
            return ret;
        return 0;
    }

    ECCODES_ASSERT(numberOfTimeRanges_);
    if ((ret = grib_get_long_internal(h, numberOfTimeRanges_, &numberOfTimeRanges)))
        return ret;
    ECCODES_ASSERT(numberOfTimeRanges == 1 || numberOfTimeRanges == 2);

    try {
        if (numberOfTimeRanges == 1) {
            ret = unpack_one_time_range_double_(val, len);
        }
        else {
            ret = unpack_multiple_time_ranges_double_(val, len);
        }
    }
    catch (std::exception& e) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "grib_accessor_g2end_step_t::unpack_double: %s", e.what());
        ret = GRIB_DECODING_ERROR;
    }

    return ret;
}

int grib_accessor_g2end_step_t::pack_long_(const long end_step_value, const long end_step_unit)
{
    grib_handle* h = grib_handle_of_accessor(this);
    int err = 0;

    long year;
    long month;
    long day;
    long hour;
    long minute;
    long second;

    long start_step_value;
    long start_step_unit;
    long time_range_unit;
    long year_of_end_of_interval;
    long month_of_end_of_interval;
    long day_of_end_of_interval;
    long hour_of_end_of_interval;
    long minute_of_end_of_interval = 0;
    long second_of_end_of_interval = 0;
    long typeOfTimeIncrement;

    double dend, dstep;
    const int show_units_for_hours = context_->grib_hourly_steps_with_units;

    eccodes::Step end_step{ end_step_value, end_step_unit };

    /*point in time */
    if (year_ == NULL) {
        if ((err = grib_set_long_internal(h, "startStepUnit", end_step.unit().value<long>())) != GRIB_SUCCESS)
            return err;
        err = grib_set_long_internal(h, start_step_value_, end_step.value<long>());
        return err;
    }

    if ((err = grib_get_long_internal(h, time_range_unit_, &time_range_unit)))
        return err;
    if ((err = grib_get_long_internal(h, year_, &year)))
        return err;
    if ((err = grib_get_long_internal(h, month_, &month)))
        return err;
    if ((err = grib_get_long_internal(h, day_, &day)))
        return err;
    if ((err = grib_get_long_internal(h, hour_, &hour)))
        return err;
    if ((err = grib_get_long_internal(h, minute_, &minute)))
        return err;
    if ((err = grib_get_long_internal(h, second_, &second)))
        return err;

    if ((err = grib_get_long_internal(h, start_step_value_, &start_step_value)))
        return err;
    if ((err = grib_get_long_internal(h, "startStepUnit", &start_step_unit)))
        return err;

    long force_step_units;
    if ((err = grib_get_long_internal(h, "forceStepUnits", &force_step_units)) != GRIB_SUCCESS)
        return err;

    if (eccodes::Unit{ start_step_unit } == eccodes::Unit{ eccodes::Unit::Value::MISSING }) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "missing start step unit");
        return GRIB_WRONG_STEP_UNIT;
    }

    if ((err = grib_get_long_internal(h, typeOfTimeIncrement_, &typeOfTimeIncrement)))
        return err;

    eccodes::Step start_step{ start_step_value, start_step_unit };
    eccodes::Step time_range = end_step - start_step;

    if (time_range.value<double>() < 0) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "endStep < startStep (%s < %s)",
                         end_step.value<std::string>("%g", show_units_for_hours).c_str(),
                         start_step.value<std::string>("%g", show_units_for_hours).c_str());
        return GRIB_WRONG_STEP;
    }

    if (!is_date_valid(year, month, day, hour, minute, second)) {  // ECC-1866
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "%s:%s: Date/Time is not valid! "
                         "year=%ld month=%ld day=%ld hour=%ld minute=%ld second=%ld",
                         class_name_, __func__, year, month, day, hour, minute, second);
        return GRIB_DECODING_ERROR;
    }

    err = grib_datetime_to_julian(year, month, day, hour, minute, second, &dend);
    if (err != GRIB_SUCCESS)
        return err;

    dstep = end_step.value<double>(eccodes::Unit{ eccodes::Unit::Value::DAY });
    dend += dstep;

    err = grib_julian_to_datetime(dend, &year_of_end_of_interval, &month_of_end_of_interval,
                                  &day_of_end_of_interval, &hour_of_end_of_interval,
                                  &minute_of_end_of_interval, &second_of_end_of_interval);
    if (err != GRIB_SUCCESS)
        return err;

    if ((err = grib_set_long_internal(h, year_of_end_of_interval_, year_of_end_of_interval)))
        return err;
    if ((err = grib_set_long_internal(h, month_of_end_of_interval_, month_of_end_of_interval)))
        return err;
    if ((err = grib_set_long_internal(h, day_of_end_of_interval_, day_of_end_of_interval)))
        return err;
    if ((err = grib_set_long_internal(h, hour_of_end_of_interval_, hour_of_end_of_interval)))
        return err;
    if ((err = grib_set_long_internal(h, minute_of_end_of_interval_, minute_of_end_of_interval)))
        return err;
    if ((err = grib_set_long_internal(h, second_of_end_of_interval_, second_of_end_of_interval)))
        return err;

    const char* forecast_time_value_key = "forecastTime";
    const char* forecast_time_unit_key  = "indicatorOfUnitOfTimeRange";
    eccodes::Step forecast_time_opt;
    eccodes::Step time_range_opt;
    if (eccodes::Unit{ force_step_units } == eccodes::Unit{ eccodes::Unit::Value::MISSING }) {
        std::tie(forecast_time_opt, time_range_opt) = find_common_units(start_step.optimize_unit(), time_range.optimize_unit());
    }
    else {
        forecast_time_opt = eccodes::Step{ start_step.value<long>(eccodes::Unit{ force_step_units }), eccodes::Unit{ force_step_units } };
        time_range_opt    = eccodes::Step{ time_range.value<long>(eccodes::Unit{ force_step_units }), eccodes::Unit{ force_step_units } };
    }

    if ((err = grib_set_long_internal(h, time_range_value_, time_range_opt.value<long>())) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(h, time_range_unit_, time_range_opt.unit().value<long>())) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(h, forecast_time_value_key, forecast_time_opt.value<long>())) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(h, forecast_time_unit_key, forecast_time_opt.unit().value<long>())) != GRIB_SUCCESS)
        return err;

    return GRIB_SUCCESS;
}

int grib_accessor_g2end_step_t::unpack_string(char* val, size_t* len)
{
    grib_handle* h       = grib_handle_of_accessor(this);
    int ret              = 0;
    char fp_format[128]  = "%g";
    size_t fp_format_len = sizeof(fp_format);
    size_t step_len      = 0;
    long step_value;
    long step_units;
    const int show_units_for_hours = context_->grib_hourly_steps_with_units;

    if ((ret = unpack_long(&step_value, &step_len)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, step_units_, &step_units)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_string(h, "formatForDoubles", fp_format, &fp_format_len)) != GRIB_SUCCESS)
        return ret;

    try {
        eccodes::Step step(step_value, step_units);
        step.set_unit(step_units);

        std::stringstream ss;

        ss << step.value<std::string>(fp_format, show_units_for_hours);

        size_t size = ss.str().size() + 1;

        if (*len < size)
            return GRIB_ARRAY_TOO_SMALL;

        *len = size;

        memcpy(val, ss.str().c_str(), size);
    }
    catch (std::exception& e) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "grib_accessor_g2end_step_t::unpack_string: %s", e.what());
        return GRIB_DECODING_ERROR;
    }

    return GRIB_SUCCESS;
}

int grib_accessor_g2end_step_t::pack_long(const long* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);
    int ret;

    long force_step_units;
    if ((ret = grib_get_long_internal(h, "forceStepUnits", &force_step_units)) != GRIB_SUCCESS)
        return ret;

    try {
        long end_step_unit;
        if (eccodes::Unit{ force_step_units } == eccodes::Unit{ eccodes::Unit::Value::MISSING }) {
            if ((ret = grib_get_long_internal(h, "endStepUnit", &end_step_unit)) != GRIB_SUCCESS)
                return ret;

            if (eccodes::Unit{ end_step_unit } == eccodes::Unit{ eccodes::Unit::Value::MISSING })
                end_step_unit = eccodes::Unit{ eccodes::Unit::Value::HOUR }.value<long>();
        }
        else {
            end_step_unit = force_step_units;
        }
        ret = pack_long_(*val, end_step_unit);
    }
    catch (std::exception& e) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "grib_accessor_g2end_step_t::pack_long: %s", e.what());
        return GRIB_DECODING_ERROR;
    }
    return ret;
}

int grib_accessor_g2end_step_t::pack_string(const char* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);
    int ret        = 0;
    long force_step_units;
    if ((ret = grib_get_long_internal(h, "forceStepUnits", &force_step_units)) != GRIB_SUCCESS)
        return ret;

    try {
        eccodes::Step end_step = step_from_string(val, eccodes::Unit{ force_step_units });
        end_step.optimize_unit();

        if ((ret = grib_set_long_internal(h, "endStepUnit", end_step.unit().value<long>())) != GRIB_SUCCESS)
            return ret;

        if ((ret = pack_long_(end_step.value<long>(), end_step.unit().value<long>())) != GRIB_SUCCESS)
            return ret;
    }
    catch (std::exception& e) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "grib_accessor_g2end_step_t::pack_string: %s", e.what());
        return GRIB_DECODING_ERROR;
    }
    return GRIB_SUCCESS;
}

long grib_accessor_g2end_step_t::get_native_type()
{
    grib_handle* h = grib_handle_of_accessor(this);
    const int show_units_for_hours = context_->grib_hourly_steps_with_units;

    if (!show_units_for_hours) {
        long step_units = 0;
        if (grib_get_long_internal(h, "stepUnits", &step_units) == GRIB_SUCCESS) {
            if (eccodes::Unit{ step_units } == eccodes::Unit::Value::HOUR) {
                return GRIB_TYPE_LONG;  // For backward compatibility
            }
        }
    }

    return GRIB_TYPE_STRING;
}
