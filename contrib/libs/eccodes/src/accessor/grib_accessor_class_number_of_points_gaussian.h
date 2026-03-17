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

class grib_accessor_number_of_points_gaussian_t : public grib_accessor_long_t
{
public:
    grib_accessor_number_of_points_gaussian_t() :
        grib_accessor_long_t() { class_name_ = "number_of_points_gaussian"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_number_of_points_gaussian_t{}; }
    int unpack_long(long* val, size_t* len) override;
    void init(const long, grib_arguments*) override;

private:
    const char* ni_ = nullptr;
    const char* nj_ = nullptr;
    const char* plpresent_ = nullptr;
    const char* pl_ = nullptr;
    const char* order_ = nullptr;
    const char* lat_first_ = nullptr;
    const char* lon_first_ = nullptr;
    const char* lat_last_ = nullptr;
    const char* lon_last_ = nullptr;
    const char* support_legacy_ = nullptr;

    int unpack_long_new(long* val, size_t* len);
    int unpack_long_with_legacy_support(long* val, size_t* len);
};
