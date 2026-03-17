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

class Modify : public Action
{
public:
    Modify(grib_context* context, const char* name, long flags);
    ~Modify() override;

    int create_accessor(grib_section*, grib_loader*) override;

    long mflags_ = 0; // modified flags
};

}  // namespace eccodes::action

grib_action* grib_action_create_modify(grib_context* context, const char* name, long flags);
