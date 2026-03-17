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

class Debug : public Dumper
{
public:
    Debug() { class_name_ = "debug"; }
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
    long begin_          = 0;
    long theEnd_         = 0;

    void set_begin_end(grib_accessor* a);
    void default_long_value(grib_accessor* a, long actualValue);
    void aliases(grib_accessor* a);
};

}  // namespace eccodes::dumper
