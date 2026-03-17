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

#include "grib_accessor_class_evaluate.h"

class grib_accessor_round_t : public grib_accessor_evaluate_t
{
public:
    grib_accessor_round_t() :
        grib_accessor_evaluate_t() { class_name_ = "round"; }
    grib_accessor* create_empty_accessor() override { return new grib_accessor_round_t{}; }
    int unpack_double(double* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    void init(const long, grib_arguments*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
};
