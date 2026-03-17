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

#include "grib_accessor_class_values.h"

class grib_accessor_data_run_length_packing_t : public grib_accessor_values_t
{
public:
    grib_accessor_data_run_length_packing_t() :
        grib_accessor_values_t() { class_name_ = "data_run_length_packing"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_run_length_packing_t{}; }
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* number_of_values_ = nullptr;
    const char* bits_per_value_ = nullptr;
    const char* max_level_value_ = nullptr;
    const char* number_of_level_values_ = nullptr;
    const char* decimal_scale_factor_ = nullptr;
    const char* level_values_ = nullptr;
};
