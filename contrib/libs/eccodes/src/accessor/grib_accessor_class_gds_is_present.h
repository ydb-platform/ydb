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

class grib_accessor_gds_is_present_t : public grib_accessor_long_t
{
public:
    grib_accessor_gds_is_present_t() :
        grib_accessor_long_t() { class_name_ = "gds_is_present"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_gds_is_present_t{}; }
    int pack_long(const long* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    void init(const long, grib_arguments*) override;

private:
    const char* gds_present_ = nullptr;
    const char* grid_definition_ = nullptr;
    const char* bitmap_present_ = nullptr;
    const char* values_ = nullptr;
};
