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

class grib_accessor_global_gaussian_t : public grib_accessor_long_t
{
public:
    grib_accessor_global_gaussian_t() :
        grib_accessor_long_t() { class_name_ = "global_gaussian"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_global_gaussian_t{}; }
    int pack_long(const long* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    void init(const long, grib_arguments*) override;

private:
    const char* N_ = nullptr;
    const char* Ni_ = nullptr;
    const char* di_ = nullptr;
    const char* latfirst_ = nullptr;
    const char* lonfirst_ = nullptr;
    const char* latlast_ = nullptr;
    const char* lonlast_ = nullptr;
    const char* plpresent_ = nullptr;
    const char* pl_ = nullptr;
    const char* basic_angle_ = nullptr;
    const char* subdivision_ = nullptr;
};
