/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_gen.h"

grib_action* grib_action_create_gen(grib_context* context, const char* name, const char* op, const long len,
                                    grib_arguments* params, grib_arguments* default_value, int flags, const char* name_space, const char* set)
{
    return new eccodes::action::Gen(context, name, op, len, params, default_value, flags, name_space, set);
}

namespace eccodes::action
{

Gen::Gen(grib_context* context, const char* name, const char* op, const long len,
         grib_arguments* params, grib_arguments* default_value, int flags, const char* name_space, const char* set)
{
    class_name_ = "action_class_gen";
    next_       = NULL;
    name_       = grib_context_strdup_persistent(context, name);
    op_         = grib_context_strdup_persistent(context, op);
    name_space_ = name_space ? grib_context_strdup_persistent(context, name_space) : nullptr;
    context_    = context;
    flags_      = flags;
#ifdef CHECK_LOWERCASE_AND_STRING_TYPE
    {
        int flag_lowercase = 0, flag_stringtype = 0;
        if (flags & GRIB_ACCESSOR_FLAG_LOWERCASE)
            flag_lowercase = 1;
        if (flags & GRIB_ACCESSOR_FLAG_STRING_TYPE)
            flag_stringtype = 1;
        if (flag_lowercase && !flag_stringtype) {
            printf("grib_action_create_gen name=%s. Has lowercase but not string_type\n", name);
            Assert(0);
        }
    }
#endif
    len_           = len;
    params_        = params;
    set_           = set ? grib_context_strdup_persistent(context, set) : nullptr;
    default_value_ = default_value;
}

Gen::~Gen()
{
    if (params_ != default_value_)
        grib_arguments_free(context_, params_);
    grib_arguments_free(context_, default_value_);

    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
    if (name_space_) {
        grib_context_free_persistent(context_, name_space_);
    }
    if (set_)
        grib_context_free_persistent(context_, set_);
    if (defaultkey_) {
        grib_context_free_persistent(context_, defaultkey_);
    }
}

void Gen::dump(FILE* f, int lvl)
{
    int i = 0;
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    grib_context_print(context_, f, "%s[%d] %s \n", op_, len_, name_);
}

// For xref implementation see
//   src/deprecated/action_class_gen.cc

int Gen::create_accessor(grib_section* p, grib_loader* loader)
{
    grib_accessor* ga = NULL;

    ga = grib_accessor_factory(p, this, len_, params_);
    if (!ga)
        return GRIB_INTERNAL_ERROR;

    grib_push_accessor(ga, p->block);

    if (ga->flags_ & GRIB_ACCESSOR_FLAG_CONSTRAINT)
        grib_dependency_observe_arguments(ga, default_value_);

    if (loader == NULL)
        return GRIB_SUCCESS;
    else
        return loader->init_accessor(loader, ga, default_value_);
}

int Gen::notify_change(grib_accessor* notified, grib_accessor* changed)
{
    if (default_value_)
        return notified->pack_expression(default_value_->get_expression(grib_handle_of_accessor(notified), 0));
    return GRIB_SUCCESS;
}

}  // namespace eccodes::action
