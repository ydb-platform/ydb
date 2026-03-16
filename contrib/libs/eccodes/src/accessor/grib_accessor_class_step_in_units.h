/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include "grib_accessor_class_long.h"

class grib_accessor_step_in_units_t : public grib_accessor_long_t
{
public:
    grib_accessor_step_in_units_t() :
        grib_accessor_long_t() { class_name_ = "step_in_units"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_step_in_units_t{}; }
    long get_native_type() override;
    int pack_long(const long* val, size_t* len) override;
    int pack_string(const char*, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* forecast_time_value_ = nullptr;
    const char* forecast_time_unit_ = nullptr;
    const char* step_units_ = nullptr;
    const char* time_range_unit_ = nullptr;
    const char* time_range_value_ = nullptr;

    int pack_long_new_(const long start_step_value, const long start_step_unit, const long force_step_units);
};
