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

class grib_accessor_julian_day_t : public grib_accessor_double_t
{
public:
    grib_accessor_julian_day_t() :
        grib_accessor_double_t() { class_name_ = "julian_day"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_julian_day_t{}; }
    int pack_double(const double* val, size_t* len) override;
    int pack_long(const long* val, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* date_ = nullptr;
    const char* hour_ = nullptr;
    const char* minute_ = nullptr;
    const char* second_ = nullptr;
};
