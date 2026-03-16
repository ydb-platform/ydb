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
#include "grib_scaling.h"

class grib_accessor_data_ccsds_packing_t : public grib_accessor_values_t
{
public:
    grib_accessor_data_ccsds_packing_t() :
        grib_accessor_values_t() { class_name_ = "data_ccsds_packing"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_ccsds_packing_t{}; }
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_float(float* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;
    int unpack_double_element(size_t i, double* val) override;
    int unpack_double_element_set(const size_t* index_array, size_t len, double* val_array) override;

private:
    const char* number_of_values_ = nullptr;
    const char* reference_value_ = nullptr;
    const char* binary_scale_factor_ = nullptr;
    const char* decimal_scale_factor_ = nullptr;
    const char* optimize_scaling_factor_ = nullptr;
    const char* bits_per_value_ = nullptr;
    const char* number_of_data_points_ = nullptr;
    const char* ccsds_flags_ = nullptr;
    const char* ccsds_block_size_ = nullptr;
    const char* ccsds_rsi_ = nullptr;

    template <typename T> int unpack(T* val, size_t* len);
};
