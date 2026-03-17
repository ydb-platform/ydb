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

#include "grib_accessor_class_data_simple_packing.h"

class grib_accessor_data_sh_unpacked_t : public grib_accessor_data_simple_packing_t
{
public:
    grib_accessor_data_sh_unpacked_t() :
        grib_accessor_data_simple_packing_t() { class_name_ = "data_sh_unpacked"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_data_sh_unpacked_t{}; }
    int unpack_double(double* val, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* GRIBEX_sh_bug_present_ = nullptr;
    const char* ieee_floats_ = nullptr;
    const char* laplacianOperatorIsSet_ = nullptr;
    const char* laplacianOperator_ = nullptr;
    const char* sub_j_ = nullptr;
    const char* sub_k_ = nullptr;
    const char* sub_m_ = nullptr;
    const char* pen_j_ = nullptr;
    const char* pen_k_ = nullptr;
    const char* pen_m_ = nullptr;
};
