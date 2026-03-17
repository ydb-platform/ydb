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

#include "grib_accessor_class_ascii.h"

class grib_accessor_cf_var_name_t : public grib_accessor_ascii_t
{
public:
    grib_accessor_cf_var_name_t() :
        grib_accessor_ascii_t() { class_name_ = "cf_var_name"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_cf_var_name_t{}; }
    int unpack_string(char*, size_t* len) override;
    size_t string_length() override;
    void init(const long, grib_arguments*) override;

private:
    const char* defaultKey_ = nullptr;
};
