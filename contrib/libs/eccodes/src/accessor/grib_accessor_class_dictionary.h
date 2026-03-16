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

class grib_accessor_dictionary_t : public grib_accessor_gen_t
{
public:
    grib_accessor_dictionary_t() :
        grib_accessor_gen_t() { class_name_ = "dictionary"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_dictionary_t{}; }
    long get_native_type() override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    int value_count(long*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;

private:
    const char* dictionary_ = nullptr;
    const char* key_ = nullptr;
    long column_ = 0;
    const char* masterDir_ = nullptr;
    const char* localDir_ = nullptr;

    grib_trie* load_dictionary(int* err);
};
