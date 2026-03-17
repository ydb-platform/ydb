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

class grib_accessor_codetable_t : public grib_accessor_unsigned_t
{
public:
    grib_accessor* create_empty_accessor() override { return new grib_accessor_codetable_t{}; }
    grib_accessor_codetable_t() :
        grib_accessor_unsigned_t() { class_name_ = "codetable"; }
    long get_native_type() override;
    int pack_missing() override;
    int pack_string(const char*, size_t* len) override;
    int pack_expression(grib_expression*) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

    grib_codetable* codetable() const { return table_; }

private:
    grib_codetable* table_ = nullptr;
    const char* tablename_ = nullptr;
    const char* masterDir_ = nullptr;
    const char* localDir_ = nullptr;
    int table_loaded_ = 0;

    grib_codetable* load_table();
};
