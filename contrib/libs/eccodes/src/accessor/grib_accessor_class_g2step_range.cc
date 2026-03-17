/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2step_range.h"
#include "step.h"
#include "step_utilities.h"
#include <iostream>

grib_accessor_g2step_range_t _grib_accessor_g2step_range{};
grib_accessor* grib_accessor_g2step_range = &_grib_accessor_g2step_range;

void grib_accessor_g2step_range_t::init(const long l, grib_arguments* c)
{
    grib_accessor_gen_t::init(l, c);

    int n = 0;

    start_step_ = c->get_name(grib_handle_of_accessor(this), n++);
    end_step_   = c->get_name(grib_handle_of_accessor(this), n++);

    length_ = 0;
}

// static void dump(grib_accessor* a, eccodes::Dumper* dumper)
//{
// dumper->dump_string(a, NULL);
//}

int grib_accessor_g2step_range_t::unpack_string(char* val, size_t* len)
{
    grib_handle* h          = grib_handle_of_accessor(this);
    int ret                 = 0;
    size_t size             = 0;
    double start_step_value = 0;
    double end_step_value   = 0;
    long step_units;

    int show_hours = context_->grib_hourly_steps_with_units;

    if ((ret = grib_get_double_internal(h, start_step_, &start_step_value)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, "stepUnits", &step_units)) != GRIB_SUCCESS)
        return ret;
    try {
        if (eccodes::Unit{ step_units } == eccodes::Unit{ eccodes::Unit::Value::MISSING }) {
            if ((ret = grib_get_long_internal(h, "stepUnits", &step_units)) != GRIB_SUCCESS)
                return ret;
        }

        char fp_format[128]  = "%g";
        size_t fp_format_len = sizeof(fp_format);
        if ((ret = grib_get_string_internal(h, "formatForDoubles", fp_format, &fp_format_len)) != GRIB_SUCCESS)
            return ret;
        std::stringstream ss;

        eccodes::Step start_step{ start_step_value, step_units };
        if (end_step_ == NULL) {
            ss << start_step.value<std::string>(fp_format, show_hours);
        }
        else {
            if ((ret = grib_get_double_internal(h, end_step_, &end_step_value)) != GRIB_SUCCESS)
                return ret;

            eccodes::Step end_step{ end_step_value, step_units };

            if (start_step_value == end_step_value) {
                ss << end_step.value<std::string>(fp_format, show_hours);
            }
            else {
                ss << start_step.value<std::string>(fp_format, show_hours) << "-" << end_step.value<std::string>(fp_format, show_hours);
            }
        }

        size = ss.str().size() + 1;

        if (*len < size)
            return GRIB_ARRAY_TOO_SMALL;

        *len = size;

        memcpy(val, ss.str().c_str(), size);
    }
    catch (std::exception& e) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_g2step_range_t::unpack_string: %s", e.what());
        return GRIB_DECODING_ERROR;
    }

    return GRIB_SUCCESS;
}

// Step range format: <start_step>[-<end_step>]
// <start_step> and <end_step> can be in different units
// stepRange="X" in instantaneous field is equivalent to set step=X
// stepRange="X" in accumulated field is equivalent to startStep=X, endStep=startStep
int grib_accessor_g2step_range_t::pack_string(const char* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);
    int ret        = 0;

    long force_step_units;
    if ((ret = grib_get_long_internal(h, "forceStepUnits", &force_step_units)) != GRIB_SUCCESS)
        return ret;

    // Note:
    // forceStepUnits is a special key that is used to identify the origin of the defined units
    // i.e., whether they have been defined by the user.
    // If this key is defined (!= 255), it indicates that the stepUnits have been defined by the user.
    // Once this key is set, it has the highest priority: it automatically overrides certain units and the default value in stepUnits

    if (h->loader) {             // h->loader is set only when rebuilding or reparsing
        force_step_units = 255;  // See ECC-1768 and ECC-1800
    }

    try {
        std::vector<eccodes::Step> steps = parse_range(val, eccodes::Unit{ force_step_units });
        if (steps.size() == 0) {
            grib_context_log(context_, GRIB_LOG_ERROR, "Could not parse step range: %s", val);
            return GRIB_INVALID_ARGUMENT;
        }

        eccodes::Step step_0;
        eccodes::Step step_1;
        if (eccodes::Unit{ force_step_units } == eccodes::Unit{ eccodes::Unit::Value::MISSING }) {
            if (steps.size() > 1)
                std::tie(step_0, step_1) = find_common_units(steps[0].optimize_unit(), steps[1].optimize_unit());
            else
                step_0 = steps[0].optimize_unit();
        }
        else {
            step_0 = eccodes::Step{ steps[0].value<long>(eccodes::Unit{ force_step_units }), eccodes::Unit{ force_step_units } };
            if (steps.size() > 1) {
                step_1 = eccodes::Step{ steps[1].value<long>(eccodes::Unit{ force_step_units }), eccodes::Unit{ force_step_units } };
            }
        }

        if ((ret = grib_set_long_internal(h, "startStepUnit", step_0.unit().value<long>())))
            return ret;
        if ((ret = set_step(h, "forecastTime", "indicatorOfUnitOfTimeRange", step_0)) != GRIB_SUCCESS)
            return ret;

        if (end_step_ != NULL) {
            if (steps.size() > 1) {
                if ((ret = grib_set_long_internal(h, "endStepUnit", step_1.unit().value<long>())))
                    return ret;
                if ((ret = grib_set_long_internal(h, end_step_, step_1.value<long>())))
                    return ret;
            }
            else {
                if ((ret = grib_set_long_internal(h, "endStepUnit", step_0.unit().value<long>())))
                    return ret;
                if ((ret = grib_set_long_internal(h, end_step_, step_0.value<long>())))
                    return ret;
            }
        }
    }
    catch (std::exception& e) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_g2step_range::pack_string: %s", e.what());
        return GRIB_INVALID_ARGUMENT;
    }
    return GRIB_SUCCESS;
}

int grib_accessor_g2step_range_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_g2step_range_t::string_length()
{
    return 255;
}

int grib_accessor_g2step_range_t::pack_long(const long* val, size_t* len)
{
    char buff[100];
    size_t bufflen = 100;

    snprintf(buff, sizeof(buff), "%ld", *val);
    return pack_string(buff, &bufflen);
}

int grib_accessor_g2step_range_t::unpack_long(long* val, size_t* len)
{
    grib_handle* h       = grib_handle_of_accessor(this);
    int ret              = 0;
    long end_start_value = 0;
    long end_step_value  = 0;
    long step_units      = 0;

    if ((ret = grib_get_long_internal(h, start_step_, &end_start_value)) != GRIB_SUCCESS)
        return ret;
    try {
        if ((ret = grib_get_long_internal(h, "stepUnits", &step_units)) != GRIB_SUCCESS)
            throw std::runtime_error("Failed to get stepUnits");
        if (eccodes::Unit{ step_units } == eccodes::Unit{ eccodes::Unit::Value::MISSING }) {
            if ((ret = grib_get_long_internal(h, "stepUnits", &step_units)) != GRIB_SUCCESS)
                return ret;
        }

        eccodes::Step start_step{ end_start_value, step_units };
        if (end_step_ == NULL) {
            *val = start_step.value<long>();
        }
        else {
            if ((ret = grib_get_long_internal(h, end_step_, &end_step_value)) != GRIB_SUCCESS)
                return ret;
            eccodes::Step end_step{ end_step_value, step_units };
            *val = end_step.value<long>();
        }
    }
    catch (std::exception& e) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Failed to unpack step range: %s", e.what());
        return GRIB_DECODING_ERROR;
    }

    return GRIB_SUCCESS;
}

int grib_accessor_g2step_range_t::unpack_double(double* val, size_t* len)
{
    grib_handle* h         = grib_handle_of_accessor(this);
    int ret                = 0;
    double end_start_value = 0;
    double end_step_value  = 0;
    long step_units        = 0;

    if ((ret = grib_get_double_internal(h, start_step_, &end_start_value)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, "stepUnits", &step_units)) != GRIB_SUCCESS)
        throw std::runtime_error("Failed to get stepUnits");

    try {
        if (eccodes::Unit{ step_units } == eccodes::Unit{ eccodes::Unit::Value::MISSING }) {
            if ((ret = grib_get_long_internal(h, "stepUnits", &step_units)) != GRIB_SUCCESS)
                return ret;
        }

        eccodes::Step start_step{ end_start_value, step_units };
        if (end_step_ == NULL) {
            *val = start_step.value<long>();
        }
        else {
            if ((ret = grib_get_double_internal(h, end_step_, &end_step_value)) != GRIB_SUCCESS)
                return ret;
            eccodes::Step end_step{ end_step_value, step_units };
            *val = end_step.value<double>();
        }
    }
    catch (std::exception& e) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grid_accessor_g2step_range::unpack_double: %s", e.what());
        return GRIB_DECODING_ERROR;
    }

    return GRIB_SUCCESS;
}

long grib_accessor_g2step_range_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}
