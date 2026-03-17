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

class SetSArray : public Action
{
public:
    SetSArray(grib_context* context, const char* name, grib_sarray* sarray);
    ~SetSArray() override;

    void dump(FILE*, int) override;
    int execute(grib_handle* h) override;

    grib_sarray* sarray_ = nullptr;
    char* name2_         = nullptr;
};

}  // namespace eccodes::action

grib_action* grib_action_create_set_sarray(grib_context* context, const char* name, grib_sarray* sarray);
