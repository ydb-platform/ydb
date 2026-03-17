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

#include <grib_api_internal.h>

namespace eccodes
{

class Dumper
{
public:
    virtual ~Dumper() {}
    virtual int init()                                                  = 0;
    virtual int destroy()                                               = 0;
    virtual void dump_long(grib_accessor*, const char*)                 = 0;
    virtual void dump_double(grib_accessor*, const char*)               = 0;
    virtual void dump_string(grib_accessor*, const char*)               = 0;
    virtual void dump_string_array(grib_accessor*, const char*)         = 0;
    virtual void dump_label(grib_accessor*, const char*)                = 0;
    virtual void dump_bytes(grib_accessor*, const char*)                = 0;
    virtual void dump_bits(grib_accessor*, const char*)                 = 0;
    virtual void dump_section(grib_accessor*, grib_block_of_accessors*) = 0;
    virtual void dump_values(grib_accessor*)                            = 0;
    virtual void header(const grib_handle*) const {};
    virtual void footer(const grib_handle*) const {};

    long count() const { return count_; }
    void count(long count) { count_ = count; }

    int depth_                  = 0;
    void* arg_                  = nullptr;
    unsigned long option_flags_ = 0;
    grib_context* context_      = nullptr;
    FILE* out_                  = nullptr;

protected:
    const char* class_name_ = nullptr;
    long count_             = 0;

private:
    // const char* name_ = nullptr;
    // size_t size_      = 0;
    // int inited_       = 0;
};

}  // namespace eccodes
