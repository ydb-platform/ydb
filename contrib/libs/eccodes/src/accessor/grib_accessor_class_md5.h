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

class grib_accessor_md5_t : public grib_accessor_gen_t
{
public:
    grib_accessor_md5_t() :
        grib_accessor_gen_t() { class_name_ = "md5"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_md5_t{}; }
    long get_native_type() override;
    int unpack_string(char*, size_t* len) override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void init(const long, grib_arguments*) override;
    int compare(grib_accessor*) override;

private:
    const char* offset_key_ = nullptr;
    grib_expression* length_key_ = nullptr;
    grib_string_list* blocklist_ = nullptr;
};
