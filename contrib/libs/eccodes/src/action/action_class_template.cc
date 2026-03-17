/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_template.h"

grib_action* grib_action_create_template(grib_context* context, int nofail, const char* name, const char* arg1, int lineno)
{
    return new eccodes::action::Template(context, nofail, name, arg1, lineno);
}

namespace eccodes::action
{

Template::Template(grib_context* context, int nofail, const char* name, const char* arg1, int lineno)
{
    class_name_ = "action_class_template";
    name_       = grib_context_strdup_persistent(context, name);
    op_         = grib_context_strdup_persistent(context, "section");
    context_    = context;
    nofail_     = nofail;

    if (arg1)
        arg_ = grib_context_strdup_persistent(context, arg1);
    else
        arg_ = nullptr;

    if (context->debug > 0 && file_being_parsed()) {
        // Construct debug information showing definition file and line
        // number of statement
        char debug_info[1024];
        const size_t infoLen = sizeof(debug_info);
        snprintf(debug_info, infoLen, "File=%s line=%d", file_being_parsed(), lineno+1);
        this->debug_info_ = grib_context_strdup_persistent(context, debug_info);
    }
}

Template::~Template()
{
    grib_context_free_persistent(context_, arg_);
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
}

void Template::dump(FILE* f, int lvl)
{
    int i = 0;
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    grib_context_print(context_, f, "Template %s  %s\n", name_, arg_);
}

static grib_action* get_empty_template(grib_context* c, int* err)
{
    char fname[]     = "empty_template.def";
    const char* path = grib_context_full_defs_path(c, fname);
    if (path) {
        *err = GRIB_SUCCESS;
        return grib_parse_file(c, path);
    }
    else {
        *err = GRIB_INTERNAL_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to get template %s", __func__, fname);
        return NULL;
    }
}

int Template::create_accessor(grib_section* p, grib_loader* h)
{
    int ret           = GRIB_SUCCESS;
    grib_action* la   = NULL;
    grib_action* next = NULL;
    grib_accessor* as = NULL;
    grib_section* gs  = NULL;

    char fname[1024] = {0, };
    char* fpath = 0;

    as = grib_accessor_factory(p, this, 0, NULL);

    if (!as)
        return GRIB_INTERNAL_ERROR;
    if (arg_) {
        ret = grib_recompose_name(p->h, as, arg_, fname, 1);

        if ((fpath = grib_context_full_defs_path(p->h->context, fname)) == NULL) {
            if (!nofail_) {
                grib_context_log(p->h->context, GRIB_LOG_ERROR,
                                 "Unable to find template %s from %s ", name_, fname);
                return GRIB_FILE_NOT_FOUND;
            }
            la = get_empty_template(p->h->context, &ret);
            if (ret)
                return ret;
        }
        else
            la = grib_parse_file(p->h->context, fpath);
    }
    as->flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;
    gs         = as->sub_section_;
    gs->branch = la; /* Will be used to prevent unnecessary reparse */

    grib_push_accessor(as, p->block);

    if (la) {
        next = la;

        while (next) {
            ret = next->create_accessor(gs, h);
            if (ret != GRIB_SUCCESS) {
                if (p->h->context->debug) {
                    grib_context_log(p->h->context, GRIB_LOG_ERROR,
                                     "Error processing template %s: %s [%s] %04lx",
                                     fname, grib_get_error_message(ret), name_, flags_);
                }
                return ret;
            }
            next = next->next_;
        }
    }
    return GRIB_SUCCESS;
}

grib_action* Template::reparse(grib_accessor* acc, int* doit)
{
    char* fpath = 0;

    if (arg_) {
        char fname[1024];
        grib_recompose_name(grib_handle_of_accessor(acc), NULL, arg_, fname, 1);

        if ((fpath = grib_context_full_defs_path(acc->context_, fname)) == NULL) {
            if (!nofail_) {
                grib_context_log(acc->context_, GRIB_LOG_ERROR,
                                 "Unable to find template %s from %s ", name_, fname);
                return NULL;
            }
            return this;
        }

        /* printf("REPARSE %s\n",fpath); */
        return grib_parse_file(acc->context_, fpath);
    }

    return NULL;
}

}  // namespace eccodes::action
