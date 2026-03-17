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

class grib_accessor_lookup_t : public grib_accessor_long_t
{
public:
    grib_accessor_lookup_t() :
        grib_accessor_long_t() { class_name_ = "lookup"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_lookup_t{}; }
    int pack_long(const long* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    long byte_count() override;
    long byte_offset() override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;
    void post_init() override;
    int notify_change(grib_accessor*) override;

private:
    long llength_ = 0;
    long loffset_ = 0;
    grib_expression* real_name_ = nullptr;
};
