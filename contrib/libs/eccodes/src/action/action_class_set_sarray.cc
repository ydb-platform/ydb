/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_set_sarray.h"


grib_action* grib_action_create_set_sarray(grib_context* context, const char* name, grib_sarray* sarray)
{
    return new eccodes::action::SetSArray(context, name, sarray);
}

namespace eccodes::action
{

SetSArray::SetSArray(grib_context* context, const char* name, grib_sarray* sarray)
{
    class_name_ = "action_class_set_sarray";
    op_         = grib_context_strdup_persistent(context, "section");
    context_    = context;
    sarray_     = sarray;
    name2_      = grib_context_strdup_persistent(context, name);

    char buf[1024];
    snprintf(buf, sizeof(buf), "set_sarray%p", (void*)sarray);

    name_ = grib_context_strdup_persistent(context, buf);
}

SetSArray::~SetSArray()
{
    grib_context_free_persistent(context_, name2_);
    grib_sarray_delete(sarray_);
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
}

int SetSArray::execute(grib_handle* h)
{
    return grib_set_string_array(h, name2_, (const char**)sarray_->v, sarray_->n);
}

void SetSArray::dump(FILE* f, int lvl)
{
    int i = 0;
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    grib_context_print(context_, f, name2_);
    printf("\n");
}

}  // namespace eccodes::action
