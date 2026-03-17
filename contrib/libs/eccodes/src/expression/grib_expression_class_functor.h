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

#include "grib_expression.h"

namespace eccodes::expression {

class Functor : public Expression {
public:
    Functor(grib_context* c, const char*, grib_arguments*);

    void destroy(grib_context*) override;
    void print(grib_context*, grib_handle*, FILE*) const override;
    void add_dependency(grib_accessor*) override;
    int native_type(grib_handle*) const override;
    int evaluate_long(grib_handle*, long*) const override;

    const char* class_name() const override { return "functor"; };

    char* name() { return name_; }  // For testing

private:
    char* name_ = nullptr;
    grib_arguments* args_ = nullptr;

    friend Expression* new_func_expression(grib_context*, const char*, grib_arguments*);
};

Expression* new_func_expression(grib_context*, const char*, grib_arguments*);

}  // namespace eccodes::expression


grib_expression* new_func_expression(grib_context*, const char*, grib_arguments*);
