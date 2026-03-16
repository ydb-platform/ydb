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

#include "action_class_section.h"

namespace eccodes::action
{

class If : public Section
{
public:
    If(grib_context* context,
       grib_expression* expression,
       grib_action* block_true, grib_action* block_false, int transient,
       int lineno, const char* file_being_parsed);
    ~If() override;

    void dump(FILE*, int) override;
    int create_accessor(grib_section*, grib_loader*) override;
    grib_action* reparse(grib_accessor* acc, int* doit) override;
    int execute(grib_handle* h) override;

    grib_expression* expression_ = nullptr;
    grib_action* block_true_     = nullptr;
    grib_action* block_false_    = nullptr;
    int transient_               = 0;
};

}  // namespace eccodes::action

grib_action* grib_action_create_if(grib_context* context,
                                   grib_expression* expression,
                                   grib_action* block_true, grib_action* block_false, int transient,
                                   int lineno, const char* file_being_parsed);
