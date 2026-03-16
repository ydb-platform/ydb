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

class grib_accessor_g1forecastmonth_t : public grib_accessor_long_t
{
public:
    grib_accessor_g1forecastmonth_t() :
        grib_accessor_long_t() { class_name_ = "g1forecastmonth"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_g1forecastmonth_t{}; }
    int pack_long(const long* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* verification_yearmonth_ = nullptr;
    const char* base_date_ = nullptr;
    const char* day_ = nullptr;
    const char* hour_ = nullptr;
    const char* fcmonth_ = nullptr;
    const char* check_ = nullptr;

    int unpack_long_edition1(long* val, size_t* len);
};
