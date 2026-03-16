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

class When : public Action
{
public:
    When(grib_context* context, grib_expression* expression, grib_action* block_true, grib_action* block_false);
    ~When() override;

    void dump(FILE*, int) override;
    int create_accessor(grib_section*, grib_loader*) override;
    int notify_change(grib_accessor* observer, grib_accessor* observed) override;

    grib_expression* expression_ = nullptr;
    grib_action* block_true_     = nullptr;
    grib_action* block_false_    = nullptr;
    int loop_                    = 0;
};

}  // namespace eccodes::action

grib_action* grib_action_create_when(grib_context* context,
                                     grib_expression* expression,
                                     grib_action* block_true, grib_action* block_false);
