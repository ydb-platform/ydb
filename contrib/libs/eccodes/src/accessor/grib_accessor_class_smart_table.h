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

#include "grib_accessor_class_unsigned.h"
#include <cctype>

class grib_accessor_smart_table_t : public grib_accessor_unsigned_t
{
public:
    grib_accessor_smart_table_t() :
        grib_accessor_unsigned_t() { class_name_ = "smart_table"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_smart_table_t{}; }
    long get_native_type() override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

    grib_smart_table* smarttable() const { return table_; }

private:
    grib_smart_table* table_ = nullptr;
    const char* values_ = nullptr;
    const char* tablename_ = nullptr;
    const char* masterDir_ = nullptr;
    const char* localDir_ = nullptr;
    const char* extraDir_ = nullptr;
    const char* extraTable_ = nullptr;
    int widthOfCode_ = 0;
    long* tableCodes_ = nullptr;
    size_t tableCodesSize_ = 0;

    grib_smart_table* load_table();
    int get_table_codes();
};
