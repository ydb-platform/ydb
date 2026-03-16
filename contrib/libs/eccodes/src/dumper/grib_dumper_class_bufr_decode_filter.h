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

class BufrDecodeFilter : public Dumper
{
public:
    BufrDecodeFilter() { class_name_ = "bufr_decode_filter"; }
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
    static inline int depth_ = 0;

    long section_offset_    = 0;
    long begin_             = 0;
    long empty_             = 0;
    //long end_               = 0;
    long isLeaf_            = 0;
    long isAttribute_       = 0;
    grib_string_list* keys_ = nullptr;

    void dump_attributes(grib_accessor* a, const char* prefix);
    void dump_values_attribute(grib_accessor* a, const char* prefix);
    void dump_long_attribute(grib_accessor* a, const char* prefix);
};

}  // namespace eccodes::dumper
