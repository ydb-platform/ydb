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

class Switch : public Section
{
public:
    Switch(grib_context* context, grib_arguments* args, grib_case* Case, grib_action* Default);
    ~Switch() override;

    int execute(grib_handle* h) override;

    grib_arguments* args_ = nullptr;
    grib_case* Case_      = nullptr;
    grib_action* Default_ = nullptr;
};

}  // namespace eccodes::action

grib_action* grib_action_create_switch(grib_context* context,
                                       grib_arguments* args,
                                       grib_case* Case,
                                       grib_action* Default);

grib_case* grib_case_new(grib_context* c, grib_arguments* values, grib_action* action);
