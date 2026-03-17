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

class grib_accessor_smart_table_column_t : public grib_accessor_gen_t
{
public:
    grib_accessor_smart_table_column_t() :
        grib_accessor_gen_t() { class_name_ = "smart_table_column"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_smart_table_column_t{}; }
    long get_native_type() override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string_array(char**, size_t* len) override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* smartTable_ = nullptr;
    int index_ = 0;
};
