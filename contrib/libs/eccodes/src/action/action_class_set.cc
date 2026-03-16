/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_set.h"

grib_action* grib_action_create_set(grib_context* context,
                                    const char* name, grib_expression* expression, int nofail)
{
    return new eccodes::action::Set(context, name, expression, nofail);
}

namespace eccodes::action
{

Set::Set(grib_context* context,
         const char* name, grib_expression* expression, int nofail)
{
    char buf[1024];

    class_name_ = "action_class_set";
    op_         = grib_context_strdup_persistent(context, "section");
    context_    = context;
    expression_ = expression;
    name2_      = grib_context_strdup_persistent(context, name);
    nofail_     = nofail;

    snprintf(buf, 1024, "set%p", (void*)expression);

    name_ = grib_context_strdup_persistent(context, buf);
}

Set::~Set()
{
    grib_context_free_persistent(context_, name_);
    expression_->destroy(context_);
    delete expression_;
    grib_context_free_persistent(context_, name2_);
    grib_context_free_persistent(context_, op_);
}

int Set::execute(grib_handle* h)
{
    int ret = 0;
    ret     = grib_set_expression(h, name2_, expression_);
    if (nofail_)
        return 0;
    if (ret != GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "Error while setting key '%s' (%s)",
                         name2_, grib_get_error_message(ret));
    }
    return ret;
}

void Set::dump(FILE* f, int lvl)
{
    int i = 0;
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    grib_context_print(context_, f, name2_);
    printf("\n");
}

}  // namespace eccodes::action
