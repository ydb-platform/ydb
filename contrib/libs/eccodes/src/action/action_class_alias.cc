/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_alias.h"


GRIB_INLINE static int grib_inline_strcmp(const char* a, const char* b)
{
    if (*a != *b)
        return 1;
    while ((*a != 0 && *b != 0) && *(a) == *(b)) {
        a++;
        b++;
    }
    return (*a == 0 && *b == 0) ? 0 : 1;
}

grib_action* grib_action_create_alias(grib_context* context, const char* name, const char* arg1, const char* name_space, int flags)
{
    return new eccodes::action::Alias(context, name, arg1, name_space, flags);
}

namespace eccodes::action
{

Alias::Alias(grib_context* context, const char* name, const char* arg1, const char* name_space, int flags)
{
    class_name_ = "action_class_alias";
    context_    = context;
    op_         = NULL;
    name_       = grib_context_strdup_persistent(context, name);
    if (name_space)
        name_space_ = grib_context_strdup_persistent(context, name_space);

    flags_  = flags;
    target_ = arg1 ? grib_context_strdup_persistent(context, arg1) : NULL;
}

Alias::~Alias()
{
    if (target_)
        grib_context_free_persistent(context_, target_);

    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
    grib_context_free_persistent(context_, name_space_);
}

static int same(const char* a, const char* b)
{
    if (a == b)
        return 1;
    if (a && b)
        return (grib_inline_strcmp(a, b) == 0);
    return 0;
}

int Alias::create_accessor(grib_section* p, grib_loader* h)
{
    int i, j, id;
    grib_accessor* x  = NULL;
    grib_accessor* y  = NULL;
    grib_handle* hand = NULL;

    /*if alias and target have the same name add only the namespace */
    if (target_ && !grib_inline_strcmp(name_, target_) && name_space_ != NULL) {
        x = grib_find_accessor_fast(p->h, target_);
        if (x == NULL) {
            grib_context_log(p->h->context, GRIB_LOG_DEBUG, "alias %s: cannot find %s (part 1)",
                             name_, target_);
            grib_context_log(p->h->context, GRIB_LOG_WARNING, "alias %s: cannot find %s",
                             name_, target_);
            return GRIB_SUCCESS;
        }

        if (x->name_space_ == NULL)
            x->name_space_ = name_space_;

        grib_context_log(p->h->context, GRIB_LOG_DEBUG, "alias: add only namespace: %s.%s",
                         name_space_, name_);
        i = 0;
        while (i < MAX_ACCESSOR_NAMES) {
            if (x->all_names_[i] != NULL && !grib_inline_strcmp(x->all_names_[i], name_)) {
                if (x->all_name_spaces_[i] == NULL) {
                    x->all_name_spaces_[i] = name_space_;
                    return GRIB_SUCCESS;
                }
                else if (!grib_inline_strcmp(x->all_name_spaces_[i], name_space_)) {
                    return GRIB_SUCCESS;
                }
            }
            i++;
        }
        i = 0;
        while (i < MAX_ACCESSOR_NAMES) {
            if (x->all_names_[i] == NULL) {
                x->all_names_[i]       = name_;
                x->all_name_spaces_[i] = name_space_;
                return GRIB_SUCCESS;
            }
            i++;
        }
        grib_context_log(p->h->context, GRIB_LOG_FATAL,
                         "unable to alias %s : increase MAX_ACCESSOR_NAMES", name_);

        return GRIB_INTERNAL_ERROR;
    }

    y = grib_find_accessor_fast(p->h, name_);

    /* delete old alias if already defined */
    if (y != NULL) {
        i = 0;
        while (i < MAX_ACCESSOR_NAMES && y->all_names_[i]) {
            if (same(y->all_names_[i], name_) && same(y->all_name_spaces_[i], name_space_)) {
                grib_context_log(p->h->context, GRIB_LOG_DEBUG, "alias %s.%s already defined for %s. Deleting old alias",
                                 name_space_, name_, y->name_);
                /* printf("[%s %s]\n",y->all_names_[i], y->all_name_spaces_[i]); */

                /*
                 * ECC-1898: Remove accessor from cache
                 * This workaround was disabled because it was causing problems with the unaliasing mars.step,
                 * i.e., when unaliasing "mars.step" it also unaliases "step"
                 */

                // TODO(maee): Implement a new hash function, which uses the name and the name_space as well

                // grib_handle* hand = grib_handle_of_accessor(y);
                // if (hand->use_trie && y->all_name_spaces_[i] != NULL && strcmp(y->name_, name_) != 0) {
                //     int id = grib_hash_keys_get_id(hand->context->keys, name_);
                //     hand->accessors[id] = NULL;
                // }

                while (i < MAX_ACCESSOR_NAMES - 1) {
                    y->all_names_[i]       = y->all_names_[i + 1];
                    y->all_name_spaces_[i] = y->all_name_spaces_[i + 1];
                    i++;
                }

                y->all_names_[MAX_ACCESSOR_NAMES - 1]       = NULL;
                y->all_name_spaces_[MAX_ACCESSOR_NAMES - 1] = NULL;

                break;
            }
            i++;
        }

        if (target_ == NULL)
            return GRIB_SUCCESS;
    }

    if (!target_)
        return GRIB_SUCCESS;

    x = grib_find_accessor_fast(p->h, target_);
    if (x == NULL) {
        grib_context_log(p->h->context, GRIB_LOG_DEBUG, "alias %s: cannot find %s (part 2)",
                         name_, target_);
        grib_context_log(p->h->context, GRIB_LOG_WARNING, "alias %s: cannot find %s",
                         name_, target_);
        return GRIB_SUCCESS;
    }

    hand = grib_handle_of_accessor(x);
    if (hand->use_trie) {
        id                  = grib_hash_keys_get_id(x->context_->keys, name_);
        hand->accessors[id] = x;

        /*
         if (hand->accessors[id] != x) {
           x->same=hand->accessors[id];
           hand->accessors[id] = x;
         }
        */
    }

    i = 0;
    while (i < MAX_ACCESSOR_NAMES) {
        if (x->all_names_[i] == NULL) {
            /* Only add entries if not already there */
            int found = 0;
            for (j = 0; j < i && !found; ++j) {
                int nameSame      = same(x->all_names_[j], name_);
                int namespaceSame = same(x->all_name_spaces_[j], name_space_);
                if (nameSame && namespaceSame) {
                    found = 1;
                }
            }
            if (!found) { /* Not there. So add them */
                x->all_names_[i]       = name_;
                x->all_name_spaces_[i] = name_space_;
                grib_context_log(p->h->context, GRIB_LOG_DEBUG, "alias %s.%s added (%s)",
                                 name_space_, name_, target_);
            }
            return GRIB_SUCCESS;
        }
        i++;
    }

    for (i = 0; i < MAX_ACCESSOR_NAMES; i++)
        grib_context_log(p->h->context, GRIB_LOG_ERROR, "alias %s= ( %s already bound to %s )",
                         name_, target_, x->all_names_[i]);

    return GRIB_SUCCESS;
}

void Alias::dump(FILE* f, int lvl)
{
    int i = 0;
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    if (target_)
        grib_context_print(context_, f, " alias %s  %s \n", name_, target_);
    else
        grib_context_print(context_, f, " unalias %s  \n", name_);
}

}  // namespace eccodes::action
