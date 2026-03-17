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

#include "grib_accessor_class_abstract_long_vector.h"

class grib_accessor_g1step_range_t : public grib_accessor_abstract_long_vector_t
{
public:
    grib_accessor_g1step_range_t() :
        grib_accessor_abstract_long_vector_t() { class_name_ = "g1step_range"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_g1step_range_t{}; }
    long get_native_type() override;
    int pack_long(const long* val, size_t* len) override;
    int pack_string(const char*, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    size_t string_length() override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

protected:
    int grib_g1_step_get_steps(long* start, long* theEnd);

private:
    const char* p1_ = nullptr;
    const char* p2_ = nullptr;
    const char* timeRangeIndicator_ = nullptr;
    const char* unit_ = nullptr;
    const char* step_unit_ = nullptr;
    const char* stepType_ = nullptr;
    const char* patch_fp_precip_ = nullptr;
    int error_on_units_ = 0;
};
