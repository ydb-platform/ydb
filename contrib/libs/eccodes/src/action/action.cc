/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action.h"

// int grib_create_accessor(grib_section* p, grib_action* a, grib_loader* h)
// {
//     /* ECC-604: Do not lock excessively */
//     // GRIB_MUTEX_INIT_ONCE(&once,&init_mutex);
//     // GRIB_MUTEX_LOCK(&mutex1);
//     auto accessor = a->create_accessor(p, h);
//     // GRIB_MUTEX_UNLOCK(&mutex1);
//     return accessor;
// }

void grib_dump_action_branch(FILE* out, grib_action* a, int decay)
{
    while (a) {
        a->dump(out, decay);
        a = a->next_;
    }
}

void grib_dump_action_tree(grib_context* ctx, FILE* out)
{
    ECCODES_ASSERT(ctx);
    ECCODES_ASSERT(ctx->grib_reader);
    ECCODES_ASSERT(ctx->grib_reader->first);
    ECCODES_ASSERT(out);

    // grib_dump_action_branch(out, ctx->grib_reader->first->root, 0);
    // grib_action* next = ctx->grib_reader->first->root;
    // while (next) {
    //     fprintf(out, "Dump %s\n", next->name);
    //     grib_dump_action_branch(out, next, 0);
    //     next = next->next;
    // }

    grib_action_file* fr = ctx->grib_reader->first;
    grib_action_file* fn = fr;
    while (fn) {
        fr = fn;
        fn = fn->next;
        grib_action* a = fr->root;
        while (a) {
            grib_action* na = a->next_;
            grib_dump_action_branch(out, a, 0);
            a = na;
        }
    }
}
