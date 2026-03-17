/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_remove.h"

grib_action* grib_action_create_remove(grib_context* context, grib_arguments* args)
{
    return new eccodes::action::Remove(context, args);
}

namespace eccodes::action
{

Remove::Remove(grib_context* context, grib_arguments* args)
{
    class_name_ = "action_class_remove";
    next_       = NULL;
    name_       = grib_context_strdup_persistent(context, "DELETE");
    op_         = grib_context_strdup_persistent(context, "remove");
    context_    = context;
    args_       = args;
}

Remove::~Remove()
{
    grib_arguments_free(context_, args_);
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
}

static void remove_accessor(grib_accessor* a)
{
    grib_section* s = NULL;
    int id;

    if (!a || !a->previous_)
        return;
    s = a->parent_;

    if (grib_handle_of_accessor(a)->use_trie && *(a->all_names_[0]) != '_') {
        id                                        = grib_hash_keys_get_id(a->context_->keys, a->all_names_[0]);
        grib_handle_of_accessor(a)->accessors[id] = NULL;
    }

    if (a->next_)
        a->previous_->next_ = a->next_;
    else
        return;

    a->next_->previous_ = a->previous_;

    a->destroy(s->h->context);
    delete a;
    a = NULL;
}

int Remove::create_accessor(grib_section* p, grib_loader* h)
{
    grib_accessor* ga = grib_find_accessor(p->h, args_->get_name(p->h, 0));

    if (ga) {
        remove_accessor(ga);
    }
    else {
        grib_context_log(context_, GRIB_LOG_DEBUG,
                         "Action_class_remove: create_accessor: No accessor named %s to remove", args_->get_name(p->h, 0));
    }
    return GRIB_SUCCESS;
}

void Remove::dump(FILE* f, int lvl)
{
    grib_context_log(context_, GRIB_LOG_ERROR, "%s: dump not implemented", name_);
    // grib_action_remove* a = (grib_action_remove*)act;
    // int i = 0;
    // for (i = 0; i < lvl; i++)
    //     grib_context_print(context_, f, "     ");
    // grib_context_print(context_, f, "remove %s as %s in %s\n",
    //     grib_arguments_get_name(0, args_, 0), name_, grib_arguments_get_name(0, args_, 1));
}

}  // namespace eccodes::action
