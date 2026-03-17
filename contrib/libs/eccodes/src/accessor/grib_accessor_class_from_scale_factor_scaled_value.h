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

#include "grib_accessor_class_double.h"

class grib_accessor_from_scale_factor_scaled_value_t : public grib_accessor_double_t
{
public:
    grib_accessor_from_scale_factor_scaled_value_t() :
        grib_accessor_double_t() { class_name_ = "from_scale_factor_scaled_value"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_from_scale_factor_scaled_value_t{}; }
    int is_missing() override;
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* scaleFactor_ = nullptr;
    const char* scaledValue_ = nullptr;
};
