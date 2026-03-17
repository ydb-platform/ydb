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

class grib_accessor_data_simple_packing_t : public grib_accessor_values_t
{
public:
    grib_accessor_data_simple_packing_t() :
        grib_accessor_values_t() { class_name_ = "data_simple_packing"; }
    //grib_accessor* create_empty_accessor() override { return new grib_accessor_data_simple_packing_t{}; }
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_float(float* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;
    int unpack_double_element(size_t i, double* val) override;
    int unpack_double_element_set(const size_t* index_array, size_t len, double* val_array) override;
    int unpack_double_subarray(double* val, size_t start, size_t len) override;

protected:
    int edition_ = 0;
    const char* units_factor_ = nullptr;
    const char* units_bias_ = nullptr;
    const char* changing_precision_ = nullptr;
    const char* number_of_values_ = nullptr;
    const char* bits_per_value_ = nullptr;
    const char* reference_value_ = nullptr;
    const char* binary_scale_factor_ = nullptr;
    const char* decimal_scale_factor_ = nullptr;
    const char* optimize_scaling_factor_ = nullptr;

private:
    template <typename T> int unpack(T* val, size_t* len);
    int _unpack_double(double* val, size_t* len, unsigned char* buf, long pos, size_t n_vals);
};
