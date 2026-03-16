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

#include "grib_api_internal.h"

namespace eccodes
{

class Action
{
public:
    virtual ~Action() {}
    virtual void dump(FILE*, int) {};
    virtual void xref(FILE* f, const char* path) {};
    virtual void destroy(grib_context*, grib_action*) {};
    virtual int create_accessor(grib_section*, grib_loader*)
    {
        fprintf(stderr, "Cannot create accessor %s %s\n", name_, class_name_);
        DEBUG_ASSERT(0);
        return 0;
    }
    virtual int notify_change(grib_accessor* observer, grib_accessor* observed)
    {
        DEBUG_ASSERT(0);
        return 0;
    };
    virtual grib_action* reparse(grib_accessor* acc, int* doit)
    {
        DEBUG_ASSERT(0);
        return nullptr;
    };
    virtual int execute(grib_handle* h)
    {
        DEBUG_ASSERT(0);
        return 0;
    }

    char* name_                    = nullptr; /**  name of the definition statement */
    char* name_space_              = nullptr; /**  namespace of the definition statement */
    char* set_                     = nullptr;
    unsigned long flags_           = 0;
    char* op_                      = nullptr; /**  operator of the definition statement */
    grib_context* context_         = nullptr; /**  Context */
    grib_action* next_             = nullptr; /**  next action in the list */
    grib_arguments* default_value_ = nullptr; /** default expression as in .def file */
    char* defaultkey_              = nullptr; /** name of the key used as default if not found */
    char* debug_info_              = nullptr; /** purely for debugging and tracing */
    const char* class_name_        = nullptr;
};

}  // namespace eccodes

using grib_action = eccodes::Action;

void grib_dump_action_branch(FILE* out, grib_action* a, int decay);
void grib_dump_action_tree(grib_context* ctx, FILE* out);
