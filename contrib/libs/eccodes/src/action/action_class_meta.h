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

#include "action_class_gen.h"

namespace eccodes::action
{

class Meta : public Gen
{
public:
    Meta(grib_context* context, const char* name, const char* op,
         grib_arguments* params, grib_arguments* default_value, unsigned long flags, const char* name_space);

    void dump(FILE*, int) override;
    int execute(grib_handle* h) override;
};

}  // namespace eccodes::action

grib_action* grib_action_create_meta(grib_context* context, const char* name, const char* op,
                                     grib_arguments* params, grib_arguments* default_value, unsigned long flags, const char* name_space);
