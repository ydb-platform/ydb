/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_set_missing.h"

grib_action* grib_action_create_set_missing(grib_context* context, const char* name)
{
    return new eccodes::action::SetMissing(context, name);
}

namespace eccodes::action
{

SetMissing::SetMissing(grib_context* context, const char* name)
{
    class_name_ = "action_class_set_missing";
    op_         = grib_context_strdup_persistent(context, "set_missing");
    context_    = context;
    name2_      = grib_context_strdup_persistent(context, name);

    char buf[1024];
    snprintf(buf, sizeof(buf), "set_missing_%s", name);

    name_ = grib_context_strdup_persistent(context, buf);
}

SetMissing::~SetMissing()
{
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, name2_);
    grib_context_free_persistent(context_, op_);
}

int SetMissing::execute(grib_handle* h)
{
    return grib_set_missing(h, name2_);
}

}  // namespace eccodes::action
