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

class Template : public Section
{
public:
    Template(grib_context* context, int nofail, const char* name, const char* arg1, int lineno);
    ~Template() override;

    void dump(FILE*, int) override;
    int create_accessor(grib_section*, grib_loader*) override;
    grib_action* reparse(grib_accessor* acc, int* doit) override;

    int nofail_ = 0;
    char* arg_  = nullptr;
};

}  // namespace eccodes::action

grib_action* grib_action_create_template(grib_context* context, int nofail, const char* name, const char* arg1, int lineno);
