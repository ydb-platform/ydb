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

#include "grib_accessor_class_data_g1simple_packing.h"

class grib_accessor_data_dummy_field_t : public grib_accessor_data_g1simple_packing_t
{
public:
    grib_accessor_data_dummy_field_t() :
        grib_accessor_data_g1simple_packing_t() { class_name_ = "data_dummy_field"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_dummy_field_t{}; }
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* missing_value_ = nullptr;
    const char* numberOfPoints_ = nullptr;
    const char* bitmap_ = nullptr;
};
