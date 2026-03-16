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

class Assert : public Action
{
public:
    Assert(grib_context* context, grib_expression* expression);
    ~Assert() override;

    int create_accessor(grib_section*, grib_loader*) override;
    int notify_change(grib_accessor* observer, grib_accessor* observed) override;
    int execute(grib_handle* h) override;

    grib_expression* expression_ = nullptr;
};

}  // namespace eccodes::action

grib_action* grib_action_create_assert(grib_context* context, grib_expression* expression);
