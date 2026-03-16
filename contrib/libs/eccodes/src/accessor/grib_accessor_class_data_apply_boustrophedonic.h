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

#include "grib_accessor_class_gen.h"

class grib_accessor_data_apply_boustrophedonic_t : public grib_accessor_gen_t
{
public:
    grib_accessor_data_apply_boustrophedonic_t() :
        grib_accessor_gen_t() { class_name_ = "data_apply_boustrophedonic"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_apply_boustrophedonic_t{}; }
    long get_native_type() override;
    int pack_double(const double* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_float(float* val, size_t* len) override;
    int value_count(long*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;
    int unpack_double_element(size_t i, double* val) override;
    int unpack_double_element_set(const size_t* index_array, size_t len, double* val_array) override;

private:
    const char* values_ = nullptr;
    const char* numberOfRows_ = nullptr;
    const char* numberOfColumns_ = nullptr;
    const char* numberOfPoints_ = nullptr;
    const char* pl_ = nullptr;

    template <typename T> int unpack(T* val, size_t* len);
};
