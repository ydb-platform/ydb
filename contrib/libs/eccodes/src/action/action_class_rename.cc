/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_rename.h"


grib_action* grib_action_create_rename(grib_context* context, char* the_old, char* the_new)
{
    return new eccodes::action::Rename(context, the_old, the_new);
}

namespace eccodes::action
{

Rename::Rename(grib_context* context, char* the_old, char* the_new)
{
    class_name_ = "action_class_rename";
    next_       = NULL;
    name_       = grib_context_strdup_persistent(context, "RENAME");
    op_         = grib_context_strdup_persistent(context, "rename");
    context_    = context;
    the_old_    = grib_context_strdup_persistent(context, the_old);
    the_new_    = grib_context_strdup_persistent(context, the_new);
}

Rename::~Rename()
{
    grib_context_free_persistent(context_, the_old_);
    grib_context_free_persistent(context_, the_new_);
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
}

static void rename_accessor(grib_accessor* a, char* name)
{
    int id;
    char* the_old = (char*)a->all_names_[0];

    if (grib_handle_of_accessor(a)->use_trie && *(a->all_names_[0]) != '_') {
        id                                        = grib_hash_keys_get_id(a->context_->keys, a->all_names_[0]);
        grib_handle_of_accessor(a)->accessors[id] = NULL;
        id                                        = grib_hash_keys_get_id(a->context_->keys, name);
        grib_handle_of_accessor(a)->accessors[id] = a;
    }
    a->all_names_[0] = grib_context_strdup_persistent(a->context_, name);
    a->name_         = a->all_names_[0];
    grib_context_log(a->context_, GRIB_LOG_DEBUG, "Renaming %s to %s", the_old, name);
    /* grib_context_free(a->context,the_old); */
}

int Rename::create_accessor(grib_section* p, grib_loader* h)
{
    grib_accessor* ga = grib_find_accessor(p->h, the_old_);

    if (ga) {
        rename_accessor(ga, the_new_);
    }
    else {
        grib_context_log(context_, GRIB_LOG_DEBUG,
                         "Action_class_rename::create_accessor: No accessor named %s to rename", the_old_);
    }

    return GRIB_SUCCESS;
}

void Rename::dump(FILE* f, int lvl)
{
    int i = 0;
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");

    grib_context_print(context_, f, "rename %s as %s in %s\n", the_old_, name_, the_new_);
}

}  // namespace eccodes::action
