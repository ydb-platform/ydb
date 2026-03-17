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

class grib_accessor_g1date_t : public grib_accessor_long_t
{
public:
    grib_accessor_g1date_t() :
        grib_accessor_long_t() { class_name_ = "g1date"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_g1date_t{}; }
    int pack_long(const long* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    int value_count(long*) override;
    void init(const long, grib_arguments*) override;

protected:
    const char* century_ = nullptr;
    const char* year_ = nullptr;
    const char* month_ = nullptr;
    const char* day_ = nullptr;
};
