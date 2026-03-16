/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Initialize projection object from string definition.  Includes
 *           pj_init(), and pj_init_plus() function.
 * Author:   Gerald Evenden, Frank Warmerdam <warmerdam@pobox.com>
 *
 ******************************************************************************
 * Copyright (c) 1995, Gerald Evenden
 * Copyright (c) 2002, Frank Warmerdam <warmerdam@pobox.com>
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

#include <ctype.h>
#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "filemanager.hpp"
#include "geodesic.h"
#include "proj.h"
#include "proj_internal.h"
#include <math.h>

/**************************************************************************************/
static paralist *string_to_paralist(PJ_CONTEXT *ctx, char *definition) {
    /***************************************************************************************
        Convert a string (presumably originating from get_init_string) to a
    paralist.
    ***************************************************************************************/
    const char *c = definition;
    paralist *first = nullptr, *last = nullptr;

    while (*c) {
        /* Keep a handle to the start of the list, so we have something to
         * return */
        auto param = pj_mkparam_ws(c, &c);
        if (nullptr == param) {
            free_params(ctx, first, PROJ_ERR_OTHER /*ENOMEM*/);
            return nullptr;
        }
        if (nullptr == last) {
            first = param;
        } else {
            last->next = param;
        }
        last = param;
    }
    return first;
}

/**************************************************************************************/
static char *get_init_string(PJ_CONTEXT *ctx, const char *name) {
/***************************************************************************************
    Read a section of an init file. Return its contents as a plain character
string. It is the duty of the caller to free the memory allocated for the
string.
***************************************************************************************/
#define MAX_LINE_LENGTH 1000
    size_t current_buffer_size = 5 * (MAX_LINE_LENGTH + 1);
    char *fname, *section;
    const char *key;
    char *buffer = nullptr;
    size_t n;

    fname = static_cast<char *>(malloc(MAX_PATH_FILENAME + ID_TAG_MAX + 3));
    if (nullptr == fname) {
        return nullptr;
    }

    /* Support "init=file:section", "+init=file:section", and "file:section"
     * format */
    key = strstr(name, "init=");
    if (nullptr == key)
        key = name;
    else
        key += 5;
    if (MAX_PATH_FILENAME + ID_TAG_MAX + 2 < strlen(key)) {
        free(fname);
        return nullptr;
    }
    memmove(fname, key, strlen(key) + 1);

    /* Locate the name of the section we search for */
    section = strrchr(fname, ':');
    if (nullptr == section) {
        pj_log(ctx, PJ_LOG_ERROR, _("Missing colon in +init"));
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        free(fname);
        return nullptr;
    }
    *section = 0;
    section++;
    n = strlen(section);
    pj_log(ctx, PJ_LOG_TRACE,
           "get_init_string: searching for section [%s] in init file [%s]",
           section, fname);

    auto file = NS_PROJ::FileManager::open_resource_file(ctx, fname);
    if (nullptr == file) {
        pj_log(ctx, PJ_LOG_ERROR, _("Cannot open %s"), fname);
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        free(fname);
        return nullptr;
    }

    /* Search for section in init file */
    std::string line;
    for (;;) {

        bool eofReached = false;
        bool maxLenReached = false;
        line = file->read_line(MAX_LINE_LENGTH, maxLenReached, eofReached);
        /* End of file? */
        if (maxLenReached || eofReached) {
            pj_log(ctx, PJ_LOG_ERROR, _("Invalid content for %s"), fname);
            proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            free(fname);
            return nullptr;
        }

        /* At start of right section? */
        pj_chomp(&line[0]);
        if ('<' != line[0])
            continue;
        if (strlen(line.c_str()) < n + 2)
            continue;
        if (line[n + 1] != '>')
            continue;
        if (0 == strncmp(line.data() + 1, section, n))
            break;
    }

    /* We're at the first line of the right section - copy line to buffer */
    buffer = static_cast<char *>(malloc(current_buffer_size));
    if (nullptr == buffer) {
        free(fname);
        return nullptr;
    }

    /* Skip the "<section>" indicator, and copy the rest of the line over */
    strcpy(buffer, line.data() + strlen(section) + 2);

    /* Copy the remaining lines of the section to buffer */
    for (;;) {
        char *end_i_cator;
        size_t next_length, buffer_length;

        /* Did the section end somewhere in the most recently read line? */
        end_i_cator = strchr(buffer, '<');
        if (end_i_cator) {
            *end_i_cator = 0;
            break;
        }

        bool eofReached = false;
        bool maxLenReached = false;
        line = file->read_line(MAX_LINE_LENGTH, maxLenReached, eofReached);
        /* End of file? - done! */
        if (maxLenReached || eofReached)
            break;

        /* Otherwise, handle the line. It MAY be the start of the next section,
         */
        /* but that will be handled at the start of next trip through the loop
         */
        buffer_length = strlen(buffer);
        pj_chomp(&line[0]); /* Remove '#' style comments */
        next_length = strlen(line.data()) + buffer_length + 2;
        if (next_length > current_buffer_size) {
            char *b = static_cast<char *>(malloc(2 * current_buffer_size));
            if (nullptr == b) {
                free(buffer);
                buffer = nullptr;
                break;
            }
            strcpy(b, buffer);
            current_buffer_size *= 2;
            free(buffer);
            buffer = b;
        }
        buffer[buffer_length] = ' ';
        strcpy(buffer + buffer_length + 1, line.data());
    }

    free(fname);
    if (nullptr == buffer)
        return nullptr;
    pj_shrink(buffer);
    pj_log(ctx, PJ_LOG_TRACE, "key=%s, value: [%s]", key, buffer);
    return buffer;
}

/************************************************************************/
static paralist *get_init(PJ_CONTEXT *ctx, const char *key,
                          int allow_init_epsg) {
    /*************************************************************************
    Expand key from buffer or (if not in buffer) from init file
    *************************************************************************/
    const char *xkey;
    char *definition = nullptr;
    paralist *init_items = nullptr;

    if (!ctx) {
        ctx = pj_get_default_ctx();
    }

    /* support "init=file:section", "+init=file:section", and "file:section"
     * format */
    xkey = strstr(key, "init=");
    if (nullptr == xkey)
        xkey = key;
    else
        xkey += 5;
    pj_log(ctx, PJ_LOG_TRACE, "get_init: searching cache for key: [%s]", xkey);

    /* Is file/key pair already in cache? */
    init_items = pj_search_initcache(xkey);
    if (init_items)
        return init_items;

    if ((strncmp(xkey, "epsg:", 5) == 0 || strncmp(xkey, "IGNF:", 5) == 0)) {
        char unused[256];
        char initname[5];
        int exists;

        strncpy(initname, xkey, 4);
        initname[4] = 0;

        if (strncmp(xkey, "epsg:", 5) == 0) {
            exists = ctx->epsg_file_exists;
            if (exists < 0) {
                exists = pj_find_file(ctx, initname, unused, sizeof(unused));
                ctx->epsg_file_exists = exists;
            }
        } else {
            exists = pj_find_file(ctx, initname, unused, sizeof(unused));
        }

        if (!exists) {
            char szInitStr[7 + 64];
            PJ *src;
            const char *proj_string;

            proj_context_errno_set(ctx, 0);

            if (!allow_init_epsg) {
                pj_log(ctx, PJ_LOG_TRACE, "%s expansion disallowed", xkey);
                return nullptr;
            }
            if (strlen(xkey) > 64) {
                return nullptr;
            }
            strcpy(szInitStr, "+init=");
            strcat(szInitStr, xkey);

            auto old_proj4_init_rules = ctx->use_proj4_init_rules;
            ctx->use_proj4_init_rules = true;
            src = proj_create(ctx, szInitStr);
            ctx->use_proj4_init_rules = old_proj4_init_rules;
            if (!src) {
                return nullptr;
            }

            proj_string = proj_as_proj_string(ctx, src, PJ_PROJ_4, nullptr);
            if (!proj_string) {
                proj_destroy(src);
                return nullptr;
            }
            definition = (char *)calloc(1, strlen(proj_string) + 1);
            if (definition) {
                strcpy(definition, proj_string);
            }

            proj_destroy(src);
        }
    }

    if (!definition) {
        /* If not, we must read it from file */
        pj_log(ctx, PJ_LOG_TRACE,
               "get_init: searching on in init files for [%s]", xkey);
        definition = get_init_string(ctx, xkey);
    }

    if (nullptr == definition)
        return nullptr;
    init_items = string_to_paralist(ctx, definition);
    if (init_items)
        pj_log(ctx, PJ_LOG_TRACE, "get_init: got [%s], paralist[0,1]: [%s,%s]",
               definition, init_items->param,
               init_items->next ? init_items->next->param : "(empty)");
    free(definition);
    if (nullptr == init_items)
        return nullptr;

    /* We found it in file - now insert into the cache, before returning */
    pj_insert_initcache(xkey, init_items);
    return init_items;
}

static void append_default_ellipsoid_to_paralist(paralist *start) {
    if (nullptr == start)
        return;

    /* Set defaults, unless inhibited (either explicitly through a "no_defs"
     * token
     */
    /* or implicitly, because we are initializing a pipeline) */
    if (pj_param_exists(start, "no_defs"))
        return;
    auto proj = pj_param_exists(start, "proj");
    if (nullptr == proj)
        return;
    if (strlen(proj->param) < 6)
        return;
    if (0 == strcmp("pipeline", proj->param + 5))
        return;

    /* Don't default ellipse if datum, ellps or any ellipsoid information is set
     */
    if (pj_param_exists(start, "datum"))
        return;
    if (pj_param_exists(start, "ellps"))
        return;
    if (pj_param_exists(start, "a"))
        return;
    if (pj_param_exists(start, "b"))
        return;
    if (pj_param_exists(start, "rf"))
        return;
    if (pj_param_exists(start, "f"))
        return;
    if (pj_param_exists(start, "e"))
        return;
    if (pj_param_exists(start, "es"))
        return;

    /* Locate end of start-list */
    paralist *last = nullptr;
    for (last = start; last->next; last = last->next)
        ;

    /* If we're here, it's OK to append the current default item */
    last->next = pj_mkparam("ellps=GRS80");
}

/*****************************************************************************/
static paralist *pj_expand_init_internal(PJ_CONTEXT *ctx, paralist *init,
                                         int allow_init_epsg) {
    /******************************************************************************
    Append expansion of <key> to the paralist <init>. The expansion is appended,
    rather than inserted at <init>'s place, since <init> may contain
    overrides to the expansion. These must take precedence, and hence come first
    in the expanded list.

    Consider e.g. the key 'foo:bar' which (hypothetically) expands to 'proj=utm
    zone=32 ellps=GRS80', i.e. a UTM projection on the GRS80 ellipsoid.

    The expression 'init=foo:bar ellps=intl' will then expand to:

               'init=foo:bar ellps=intl proj=utm zone=32 ellps=GRS80',

    where 'ellps=intl' precedes 'ellps=GRS80', and hence takes precedence,
    turning the expansion into an UTM projection on the Hayford ellipsoid.

    Note that 'init=foo:bar' stays in the list. It is ignored after expansion.

    ******************************************************************************/
    paralist *last;
    paralist *expn;

    /* Nowhere to start? */
    if (nullptr == init)
        return nullptr;

    expn = get_init(ctx, init->param, allow_init_epsg);

    /* Nothing in expansion? */
    if (nullptr == expn)
        return nullptr;

    /* Locate  the end of the list */
    for (last = init; last && last->next; last = last->next)
        ;

    /* Then append and return */
    last->next = expn;
    return init;
}

paralist *pj_expand_init(PJ_CONTEXT *ctx, paralist *init) {
    return pj_expand_init_internal(ctx, init, TRUE);
}

/************************************************************************/
/*                              pj_init()                               */
/*                                                                      */
/*      Main entry point for initialing a PJ projections                */
/*      definition.  Note that the projection specific function is      */
/*      called to do the initial allocation so it can be created        */
/*      large enough to hold projection specific parameters.            */
/************************************************************************/

static PJ_CONSTRUCTOR locate_constructor(const char *name) {
    int i;
    const char *s;
    const PJ_OPERATIONS *operations;
    operations = proj_list_operations();
    for (i = 0; (s = operations[i].id) && strcmp(name, s); ++i)
        ;
    if (nullptr == s)
        return nullptr;
    return (PJ_CONSTRUCTOR)operations[i].proj;
}

PJ *pj_init_ctx_with_allow_init_epsg(PJ_CONTEXT *ctx, int argc, char **argv,
                                     int allow_init_epsg) {
    const char *s;
    char *name;
    PJ_CONSTRUCTOR proj;
    paralist *curr, *init, *start;
    int i;
    int err;
    PJ *PIN = nullptr;
    int n_pipelines = 0;
    int n_inits = 0;
    const PJ_UNITS *units;
    const PJ_PRIME_MERIDIANS *prime_meridians;

    if (nullptr == ctx)
        ctx = pj_get_default_ctx();

    ctx->last_errno = 0;

    if (argc <= 0) {
        pj_log(ctx, PJ_LOG_ERROR, _("No arguments"));
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_MISSING_ARG);
        return nullptr;
    }

    /* count occurrences of pipelines and inits */
    for (i = 0; i < argc; ++i) {
        if (!strcmp(argv[i], "+proj=pipeline") ||
            !strcmp(argv[i], "proj=pipeline"))
            n_pipelines++;
        if (!strncmp(argv[i], "+init=", 6) || !strncmp(argv[i], "init=", 5))
            n_inits++;
    }

    /* can't have nested pipelines directly */
    if (n_pipelines > 1) {
        pj_log(ctx, PJ_LOG_ERROR, _("Nested pipelines are not supported"));
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
        return nullptr;
    }

    /* don't allow more than one +init in non-pipeline operations */
    if (n_pipelines == 0 && n_inits > 1) {
        pj_log(ctx, PJ_LOG_ERROR, _("Too many inits"));
        proj_context_errno_set(ctx, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
        return nullptr;
    }

    /* put arguments into internal linked list */
    start = curr = pj_mkparam(argv[0]);
    if (!curr) {
        free_params(ctx, start, PROJ_ERR_OTHER /*ENOMEM*/);
        return nullptr;
    }

    for (i = 1; i < argc; ++i) {
        curr->next = pj_mkparam(argv[i]);
        if (!curr->next) {
            free_params(ctx, start, PROJ_ERR_OTHER /*ENOMEM*/);
            return nullptr;
        }
        curr = curr->next;
    }

    /* Only expand '+init's in non-pipeline operations. '+init's in pipelines
     * are
     */
    /* expanded in the individual pipeline steps during pipeline initialization.
     */
    /* Potentially this leads to many nested pipelines, which shouldn't be a */
    /* problem when '+init's are expanded as late as possible. */
    init = pj_param_exists(start, "init");
    if (init && n_pipelines == 0) {
        init = pj_expand_init_internal(ctx, init, allow_init_epsg);
        if (!init) {
            free_params(ctx, start, PROJ_ERR_INVALID_OP_WRONG_SYNTAX);
            return nullptr;
        }
    }
    if (ctx->last_errno) {
        free_params(ctx, start, ctx->last_errno);
        return nullptr;
    }

    /* Find projection selection */
    curr = pj_param_exists(start, "proj");
    if (nullptr == curr) {
        pj_log(ctx, PJ_LOG_ERROR, _("Missing proj"));
        free_params(ctx, start, PROJ_ERR_INVALID_OP_MISSING_ARG);
        return nullptr;
    }
    name = curr->param;
    if (strlen(name) < 6) {
        pj_log(ctx, PJ_LOG_ERROR, _("Invalid value for proj"));
        free_params(ctx, start, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        return nullptr;
    }
    name += 5;

    proj = locate_constructor(name);
    if (nullptr == proj) {
        pj_log(ctx, PJ_LOG_ERROR, _("Unknown projection"));
        free_params(ctx, start, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        return nullptr;
    }

    append_default_ellipsoid_to_paralist(start);

    /* Allocate projection structure */
    PIN = proj(nullptr);
    if (nullptr == PIN) {
        free_params(ctx, start, PROJ_ERR_OTHER /*ENOMEM*/);
        return nullptr;
    }

    PIN->ctx = ctx;
    PIN->params = start;
    PIN->is_latlong = 0;
    PIN->is_geocent = 0;
    PIN->is_long_wrap_set = 0;
    PIN->long_wrap_center = 0.0;
    strcpy(PIN->axis, "enu");

    /* Set datum parameters. Similarly to +init parameters we want to expand */
    /* +datum parameters as late as possible when dealing with pipelines. */
    /* otherwise only the first occurrence of +datum will be expanded and that
     */
    if (n_pipelines == 0) {
        if (pj_datum_set(ctx, start, PIN))
            return pj_default_destructor(PIN, proj_errno(PIN));
    }

    err = pj_ellipsoid(PIN);

    if (err) {
        /* Didn't get an ellps, but doesn't need one: Get a free WGS84 */
        if (PIN->need_ellps) {
            pj_log(ctx, PJ_LOG_ERROR,
                   _("pj_init_ctx: Must specify ellipsoid or sphere"));
            return pj_default_destructor(PIN, proj_errno(PIN));
        } else {
            if (PIN->a == 0)
                proj_errno_reset(PIN);
            PIN->f = 1.0 / 298.257223563;
            PIN->a = 6378137.0;
            PIN->es = PIN->f * (2 - PIN->f);
        }
    }
    PIN->a_orig = PIN->a;
    PIN->es_orig = PIN->es;
    if (pj_calc_ellipsoid_params(PIN, PIN->a, PIN->es))
        return pj_default_destructor(PIN,
                                     PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);

    /* Now that we have ellipse information check for WGS84 datum */
    if (PIN->datum_type == PJD_3PARAM && PIN->datum_params[0] == 0.0 &&
        PIN->datum_params[1] == 0.0 && PIN->datum_params[2] == 0.0 &&
        PIN->a == 6378137.0 &&
        ABS(PIN->es - 0.006694379990) < 0.000000000050) /*WGS84/GRS80*/
    {
        PIN->datum_type = PJD_WGS84;
    }

    /* Set PIN->geoc coordinate system */
    PIN->geoc = (PIN->es != 0.0 && pj_param(ctx, start, "bgeoc").i);

    /* Over-ranging flag */
    PIN->over = pj_param(ctx, start, "bover").i;
    if (ctx->forceOver) {
        PIN->over = ctx->forceOver;
    }

    /* Vertical datum geoid grids */
    PIN->has_geoid_vgrids = pj_param(ctx, start, "tgeoidgrids").i;
    if (PIN->has_geoid_vgrids) /* we need to mark it as used. */
        pj_param(ctx, start, "sgeoidgrids");

    /* Longitude center for wrapping */
    PIN->is_long_wrap_set = pj_param(ctx, start, "tlon_wrap").i;
    if (PIN->is_long_wrap_set) {
        PIN->long_wrap_center = pj_param(ctx, start, "rlon_wrap").f;
        /* Don't accept excessive values otherwise we might perform badly */
        /* when correcting longitudes around it */
        /* The test is written this way to error on long_wrap_center "=" NaN */
        if (!(fabs(PIN->long_wrap_center) < 10 * M_TWOPI)) {
            proj_log_error(PIN, _("Invalid value for lon_wrap"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    /* Axis orientation */
    if ((pj_param(ctx, start, "saxis").s) != nullptr) {
        const char *axis_legal = "ewnsud";
        const char *axis_arg = pj_param(ctx, start, "saxis").s;
        if (strlen(axis_arg) != 3) {
            proj_log_error(PIN, _("Invalid value for axis"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        if (strchr(axis_legal, axis_arg[0]) == nullptr ||
            strchr(axis_legal, axis_arg[1]) == nullptr ||
            strchr(axis_legal, axis_arg[2]) == nullptr) {
            proj_log_error(PIN, _("Invalid value for axis"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        /* TODO: it would be nice to validate we don't have on axis repeated */
        strcpy(PIN->axis, axis_arg);
    }

    /* Central meridian */
    PIN->lam0 = pj_param(ctx, start, "rlon_0").f;

    /* Central latitude */
    PIN->phi0 = pj_param(ctx, start, "rlat_0").f;
    if (fabs(PIN->phi0) > M_HALFPI) {
        proj_log_error(PIN,
                       _("Invalid value for lat_0: |lat_0| should be <= 90°"));
        return pj_default_destructor(PIN,
                                     PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    /* False easting and northing */
    PIN->x0 = pj_param(ctx, start, "dx_0").f;
    PIN->y0 = pj_param(ctx, start, "dy_0").f;
    PIN->z0 = pj_param(ctx, start, "dz_0").f;
    PIN->t0 = pj_param(ctx, start, "dt_0").f;

    /* General scaling factor */
    if (pj_param(ctx, start, "tk_0").i)
        PIN->k0 = pj_param(ctx, start, "dk_0").f;
    else if (pj_param(ctx, start, "tk").i)
        PIN->k0 = pj_param(ctx, start, "dk").f;
    else
        PIN->k0 = 1.;
    if (PIN->k0 <= 0.) {
        proj_log_error(PIN, _("Invalid value for k/k_0: it should be > 0"));
        return pj_default_destructor(PIN,
                                     PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    /* Set units */
    units = pj_list_linear_units();
    s = nullptr;
    if ((name = pj_param(ctx, start, "sunits").s) != nullptr) {
        for (i = 0; (s = units[i].id) && strcmp(name, s); ++i)
            ;
        if (!s) {
            proj_log_error(PIN, _("Invalid value for units"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        s = units[i].to_meter;
    }
    if (s || (s = pj_param(ctx, start, "sto_meter").s)) {
        char *end_ptr = const_cast<char *>(s);
        PIN->to_meter = pj_strtod(s, &end_ptr);
        s = end_ptr;
        if (*s == '/') { /* ratio number */
            ++s;
            double denom = pj_strtod(s, nullptr);
            if (denom == 0.0) {
                proj_log_error(PIN,
                               _("Invalid value for to_meter donominator"));
                return pj_default_destructor(
                    PIN, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
            PIN->to_meter /= denom;
        }
        if (PIN->to_meter <= 0.0) {
            proj_log_error(PIN, _("Invalid value for to_meter"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        PIN->fr_meter = 1 / PIN->to_meter;

    } else
        PIN->to_meter = PIN->fr_meter = 1.;

    /* Set vertical units */
    s = nullptr;
    if ((name = pj_param(ctx, start, "svunits").s) != nullptr) {
        for (i = 0; (s = units[i].id) && strcmp(name, s); ++i)
            ;
        if (!s) {
            proj_log_error(PIN, _("Invalid value for vunits"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        s = units[i].to_meter;
    }
    if (s || (s = pj_param(ctx, start, "svto_meter").s)) {
        char *end_ptr = const_cast<char *>(s);
        PIN->vto_meter = pj_strtod(s, &end_ptr);
        s = end_ptr;
        if (*s == '/') { /* ratio number */
            ++s;
            double denom = pj_strtod(s, nullptr);
            if (denom == 0.0) {
                proj_log_error(PIN,
                               _("Invalid value for vto_meter donominator"));
                return pj_default_destructor(
                    PIN, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
            PIN->vto_meter /= denom;
        }
        if (PIN->vto_meter <= 0.0) {
            proj_log_error(PIN, _("Invalid value for vto_meter"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        PIN->vfr_meter = 1. / PIN->vto_meter;
    } else {
        PIN->vto_meter = PIN->to_meter;
        PIN->vfr_meter = PIN->fr_meter;
    }

    /* Prime meridian */
    prime_meridians = proj_list_prime_meridians();
    s = nullptr;
    if ((name = pj_param(ctx, start, "spm").s) != nullptr) {
        const char *value = nullptr;
        char *next_str = nullptr;

        for (i = 0; prime_meridians[i].id != nullptr; ++i) {
            if (strcmp(name, prime_meridians[i].id) == 0) {
                value = prime_meridians[i].defn;
                break;
            }
        }

        if (value == nullptr &&
            (dmstor_ctx(ctx, name, &next_str) != 0.0 || *name == '0') &&
            *next_str == '\0')
            value = name;

        if (!value) {
            proj_log_error(PIN, _("Invalid value for pm"));
            return pj_default_destructor(PIN,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        PIN->from_greenwich = dmstor_ctx(ctx, value, nullptr);
    } else
        PIN->from_greenwich = 0.0;

    /* Private object for the geodesic functions */
    PIN->geod = static_cast<struct geod_geodesic *>(
        calloc(1, sizeof(struct geod_geodesic)));
    if (nullptr == PIN->geod)
        return pj_default_destructor(PIN, PROJ_ERR_OTHER /*ENOMEM*/);
    geod_init(PIN->geod, PIN->a, PIN->f);

    /* Projection specific initialization */
    err = proj_errno_reset(PIN);
    PIN = proj(PIN);
    if (proj_errno(PIN)) {
        proj_destroy(PIN);
        return nullptr;
    }
    proj_errno_restore(PIN, err);
    return PIN;
}
