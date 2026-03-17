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

#include "action.h"

namespace eccodes::action
{

class Print : public Action
{
public:
    Print(grib_context* context, const char* name, char* outname);
    ~Print() override;

    int create_accessor(grib_section*, grib_loader*) override;
    int execute(grib_handle* h) override;

    char* name2_ = nullptr;  // TODO(maee): find a better member variable name
    char* outname_  = nullptr;
};

}  // namespace eccodes::action

grib_action* grib_action_create_print(grib_context* context, const char* name, char* outname);
