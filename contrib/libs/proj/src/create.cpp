/******************************************************************************
 * Project:  PROJ
 * Purpose:  pj_create_internal() related stuff
 *
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2016-06-09/2016-11-06
 *
 ******************************************************************************
 * Copyright (c) 2016, 2017 Thomas Knudsen/SDFE
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#define FROM_PROJ_CPP

#include "proj.h"
#include "proj_internal.h"
#include <math.h>

/*************************************************************************************/
static PJ *skip_prep_fin(PJ *P) {
    /**************************************************************************************
    Skip prepare and finalize function for the various "helper operations" added
    to P when in cs2cs compatibility mode.
    **************************************************************************************/
    P->skip_fwd_prepare = 1;
    P->skip_fwd_finalize = 1;
    P->skip_inv_prepare = 1;
    P->skip_inv_finalize = 1;
    return P;
}

/*************************************************************************************/
static int cs2cs_emulation_setup(PJ *P) {
    /**************************************************************************************
    If any cs2cs style modifiers are given (axis=..., towgs84=..., ) create the
    4D API equivalent operations, so the preparation and finalization steps in
    the pj_inv/pj_fwd invocators can emulate the behavior of pj_transform and
    the cs2cs app.

    Returns 1 on success, 0 on failure
    **************************************************************************************/
    PJ *Q;
    paralist *p;
    int do_cart = 0;
    if (nullptr == P)
        return 0;

    /* Don't recurse when calling proj_create (which calls us back) */
    if (pj_param_exists(P->params, "break_cs2cs_recursion"))
        return 1;

    /* Swap axes? */
    p = pj_param_exists(P->params, "axis");

    const bool disable_grid_presence_check =
        pj_param_exists(P->params, "disable_grid_presence_check") != nullptr;

    /* Don't axisswap if data are already in "enu" order */
    if (p && (0 != strcmp("enu", p->param))) {
        size_t def_size = 100 + strlen(P->axis);
        char *def = static_cast<char *>(malloc(def_size));
        if (nullptr == def)
            return 0;
        snprintf(def, def_size,
                 "break_cs2cs_recursion     proj=axisswap  axis=%s", P->axis);
        Q = pj_create_internal(P->ctx, def);
        free(def);
        if (nullptr == Q)
            return 0;
        P->axisswap = skip_prep_fin(Q);
    }

    /* Geoid grid(s) given? */
    p = pj_param_exists(P->params, "geoidgrids");
    if (!disable_grid_presence_check && p &&
        strlen(p->param) > strlen("geoidgrids=")) {
        char *gridnames = p->param + strlen("geoidgrids=");
        size_t def_size = 100 + 2 * strlen(gridnames);
        char *def = static_cast<char *>(malloc(def_size));
        if (nullptr == def)
            return 0;
        snprintf(def, def_size,
                 "break_cs2cs_recursion     proj=vgridshift  grids=%s",
                 pj_double_quote_string_param_if_needed(gridnames).c_str());
        Q = pj_create_internal(P->ctx, def);
        free(def);
        if (nullptr == Q)
            return 0;
        P->vgridshift = skip_prep_fin(Q);
    }

    /* Datum shift grid(s) given? */
    p = pj_param_exists(P->params, "nadgrids");
    if (!disable_grid_presence_check && p &&
        strlen(p->param) > strlen("nadgrids=")) {
        char *gridnames = p->param + strlen("nadgrids=");
        size_t def_size = 100 + 2 * strlen(gridnames);
        char *def = static_cast<char *>(malloc(def_size));
        if (nullptr == def)
            return 0;
        snprintf(def, def_size,
                 "break_cs2cs_recursion     proj=hgridshift  grids=%s",
                 pj_double_quote_string_param_if_needed(gridnames).c_str());
        Q = pj_create_internal(P->ctx, def);
        free(def);
        if (nullptr == Q)
            return 0;
        P->hgridshift = skip_prep_fin(Q);
    }

    /* We ignore helmert if we have grid shift */
    p = P->hgridshift ? nullptr : pj_param_exists(P->params, "towgs84");
    while (p) {
        const char *const s = p->param;
        const double *const d = P->datum_params;

        /* We ignore null helmert shifts (common in auto-translated resource
         * files, e.g. epsg) */
        if (0 == d[0] && 0 == d[1] && 0 == d[2] && 0 == d[3] && 0 == d[4] &&
            0 == d[5] && 0 == d[6]) {
            /* If the current ellipsoid is not WGS84, then make sure the */
            /* change in ellipsoid is still done. */
            if (!(fabs(P->a_orig - 6378137.0) < 1e-8 &&
                  fabs(P->es_orig - 0.0066943799901413) < 1e-15)) {
                do_cart = 1;
            }
            break;
        }

        const size_t n = strlen(s);
        if (n <= 8) /* 8==strlen ("towgs84=") */
            return 0;

        const size_t def_max_size = 100 + n;
        std::string def;
        def.reserve(def_max_size);
        def += "break_cs2cs_recursion     proj=helmert exact ";
        def += s;
        def += " convention=position_vector";
        Q = pj_create_internal(P->ctx, def.c_str());
        if (nullptr == Q)
            return 0;
        pj_inherit_ellipsoid_def(P, Q);
        P->helmert = skip_prep_fin(Q);

        break;
    }

    /* We also need cartesian/geographical transformations if we are working in
     */
    /* geocentric/cartesian space or we need to do a Helmert transform. */
    if (P->is_geocent || P->helmert || do_cart) {
        char def[150];
        snprintf(def, sizeof(def),
                 "break_cs2cs_recursion     proj=cart   a=%40.20g  es=%40.20g",
                 P->a_orig, P->es_orig);
        {
            /* In case the current locale does not use dot but comma as decimal
             */
            /* separator, replace it with dot, so that proj_atof() behaves */
            /* correctly. */
            /* TODO later: use C++ ostringstream with
             * imbue(std::locale::classic()) */
            /* to be locale unaware */
            char *next_pos;
            for (next_pos = def; (next_pos = strchr(next_pos, ',')) != nullptr;
                 next_pos++) {
                *next_pos = '.';
            }
        }
        Q = pj_create_internal(P->ctx, def);
        if (nullptr == Q)
            return 0;
        P->cart = skip_prep_fin(Q);

        if (!P->is_geocent) {
            snprintf(def, sizeof(def),
                     "break_cs2cs_recursion     proj=cart  ellps=WGS84");
            Q = pj_create_internal(P->ctx, def);
            if (nullptr == Q)
                return 0;
            P->cart_wgs84 = skip_prep_fin(Q);
        }
    }

    return 1;
}

/*************************************************************************************/
PJ *pj_create_internal(PJ_CONTEXT *ctx, const char *definition) {
    /*************************************************************************************/

    /**************************************************************************************
        Create a new PJ object in the context ctx, using the given definition.
    If ctx==0, the default context is used, if definition==0, or invalid, a
    null-pointer is returned. The definition may use '+' as argument start
    indicator, as in
        "+proj=utm +zone=32", or leave it out, as in "proj=utm zone=32".

        It may even use free formatting "proj  =  utm;  zone  =32  ellps=
    GRS80". Note that the semicolon separator is allowed, but not required.
    **************************************************************************************/
    char *args, **argv;
    size_t argc, n;

    if (nullptr == ctx)
        ctx = pj_get_default_ctx();

    /* Make a copy that we can manipulate */
    n = strlen(definition);
    args = (char *)malloc(n + 1);
    if (nullptr == args) {
        proj_context_errno_set(ctx, PROJ_ERR_OTHER /*ENOMEM*/);
        return nullptr;
    }
    strcpy(args, definition);

    argc = pj_trim_argc(args);
    if (argc == 0) {
        free(args);
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_MISSING_ARG);
        return nullptr;
    }

    argv = pj_trim_argv(argc, args);
    if (!argv) {
        free(args);
        proj_context_errno_set(ctx, PROJ_ERR_OTHER /*ENOMEM*/);
        return nullptr;
    }

    PJ *P = pj_create_argv_internal(ctx, (int)argc, argv);

    free(argv);
    free(args);

    return P;
}

/*************************************************************************************/
PJ *proj_create_argv(PJ_CONTEXT *ctx, int argc, char **argv) {
    /**************************************************************************************
    Create a new PJ object in the context ctx, using the given definition
    argument array argv. If ctx==0, the default context is used, if
    definition==0, or invalid, a null-pointer is returned. The definition
    arguments may use '+' as argument start indicator, as in {"+proj=utm",
    "+zone=32"}, or leave it out, as in {"proj=utm", "zone=32"}.
    **************************************************************************************/

    if (nullptr == ctx)
        ctx = pj_get_default_ctx();
    if (nullptr == argv) {
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_MISSING_ARG);
        return nullptr;
    }

    /* We assume that free format is used, and build a full proj_create
     * compatible string */
    char *c = pj_make_args(argc, argv);
    if (nullptr == c) {
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP /* ENOMEM */);
        return nullptr;
    }

    PJ *P = proj_create(ctx, c);

    free((char *)c);
    return P;
}

/*************************************************************************************/
PJ *pj_create_argv_internal(PJ_CONTEXT *ctx, int argc, char **argv) {
    /**************************************************************************************
    For use by pipeline init function.
    **************************************************************************************/
    if (nullptr == ctx)
        ctx = pj_get_default_ctx();
    if (nullptr == argv) {
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_MISSING_ARG);
        return nullptr;
    }

    /* ...and let pj_init_ctx do the hard work */
    /* New interface: forbid init=epsg:XXXX syntax by default */
    const int allow_init_epsg =
        proj_context_get_use_proj4_init_rules(ctx, FALSE);
    PJ *P = pj_init_ctx_with_allow_init_epsg(ctx, argc, argv, allow_init_epsg);

    /* Support cs2cs-style modifiers */
    int ret = cs2cs_emulation_setup(P);
    if (0 == ret)
        return proj_destroy(P);

    return P;
}
