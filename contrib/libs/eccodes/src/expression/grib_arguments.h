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

#include "grib_api_internal.h"
#include "grib_expression.h"

namespace eccodes {

class Arguments
{
public:
    Arguments(grib_context* c, Expression* g, Arguments* n);
    ~Arguments();

    void print(grib_handle* f) const;
    const char* get_name(grib_handle* h, int n) const;
    const char* get_string(grib_handle* h, int n) const;
    long get_long(grib_handle* h, int n) const;
    double get_double(grib_handle* h, int n) const;
    grib_expression* get_expression(grib_handle* h, int n) const;
    int get_count() const;

    // TODO(maee): make this private
    // private:
    Arguments* next_        = nullptr;
    Expression* expression_ = nullptr;
    grib_context* context_  = nullptr;
};

}  // namespace eccodes

eccodes::Arguments* grib_arguments_new(grib_context* c, grib_expression* g, eccodes::Arguments* n);
void grib_arguments_free(grib_context* c, grib_arguments* g);
