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

class grib_accessor_unsigned_t : public grib_accessor_long_t
{
public:
    grib_accessor_unsigned_t() :
        grib_accessor_long_t() { class_name_ = "None"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_unsigned_t{}; }
    void init(const long len, grib_arguments* arg) override;
    void dump(eccodes::Dumper* dumper) override;
    int unpack_long(long* val, size_t* len) override;
    int pack_long(const long* val, size_t* len) override;
    long byte_count() override;
    int value_count(long* len) override;
    long byte_offset() override;
    void update_size(size_t s) override;
    long next_offset() override;
    int is_missing() override;
    void destroy(grib_context* context) override;

protected:
    long nbytes_ = 0;

    int pack_long_unsigned_helper(const long* val, size_t* len, int check);

private:
    grib_arguments* arg_ = nullptr;

};
