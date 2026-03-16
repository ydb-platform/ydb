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

#include "grib_dumper.h"

namespace eccodes::dumper
{

class Default : public Dumper
{
public:
    Default() { class_name_ = "default"; }
    int init() override;
    int destroy() override;
    void dump_long(grib_accessor* a, const char* comment) override;
    void dump_bits(grib_accessor* a, const char* comment) override;
    void dump_double(grib_accessor* a, const char* comment) override;
    void dump_string(grib_accessor* a, const char* comment) override;
    void dump_string_array(grib_accessor* a, const char* comment) override;
    void dump_bytes(grib_accessor* a, const char* comment) override;
    void dump_values(grib_accessor* a) override;
    void dump_label(grib_accessor* a, const char* comment) override;
    void dump_section(grib_accessor* a, grib_block_of_accessors* block) override;

private:
    long section_offset_ = 0;

    void aliases(grib_accessor* a);
    void print_offset(FILE* out, grib_accessor* a);
};

}  // namespace eccodes::dumper
