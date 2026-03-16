/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  This is primarily material originating from pj_obs_api.c
 *           (now proj_4D_api.c), that does not fit into the API
 *           category. Hence this pile of tubings and fittings for
 *           PROJ.4 internal plumbing.
 *
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2017-07-05
 *
 ******************************************************************************
 * Copyright (c) 2016, 2017, 2018, Thomas Knudsen/SDFE
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

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "geodesic.h"
#include "proj_internal.h"

#include "proj/internal/internal.hpp"

using namespace NS_PROJ::internal;

enum pj_io_units pj_left(PJ *P) {
    enum pj_io_units u = P->inverted ? P->right : P->left;
    if (u == PJ_IO_UNITS_CLASSIC)
        return PJ_IO_UNITS_PROJECTED;
    return u;
}

enum pj_io_units pj_right(PJ *P) {
    enum pj_io_units u = P->inverted ? P->left : P->right;
    if (u == PJ_IO_UNITS_CLASSIC)
        return PJ_IO_UNITS_PROJECTED;
    return u;
}

/* Work around non-constness of MSVC HUGE_VAL by providing functions rather than
 * constants */
PJ_COORD proj_coord_error(void) {
    PJ_COORD c;
    c.v[0] = c.v[1] = c.v[2] = c.v[3] = HUGE_VAL;
    return c;
}

/**************************************************************************************/
PJ_COORD pj_approx_2D_trans(PJ *P, PJ_DIRECTION direction, PJ_COORD coo) {
    /***************************************************************************************
    Behave mostly as proj_trans, but attempt to use 2D interfaces only.
    Used in gie.c, to enforce testing 2D code, and by PJ_pipeline.c to implement
    chained calls starting out with a call to its 2D interface.
    ***************************************************************************************/
    if (nullptr == P)
        return coo;
    if (P->inverted)
        direction = static_cast<PJ_DIRECTION>(-direction);
    switch (direction) {
    case PJ_FWD: {
        const auto xy = pj_fwd(coo.lp, P);
        coo.xy = xy;
        return coo;
    }
    case PJ_INV: {
        const auto lp = pj_inv(coo.xy, P);
        coo.lp = lp;
        return coo;
    }
    case PJ_IDENT:
        break;
    }
    return coo;
}

/**************************************************************************************/
PJ_COORD pj_approx_3D_trans(PJ *P, PJ_DIRECTION direction, PJ_COORD coo) {
    /***************************************************************************************
    Companion to pj_approx_2D_trans.

    Behave mostly as proj_trans, but attempt to use 3D interfaces only.
    Used in gie.c, to enforce testing 3D code, and by PJ_pipeline.c to implement
    chained calls starting out with a call to its 3D interface.
    ***************************************************************************************/
    if (nullptr == P)
        return coo;
    if (P->inverted)
        direction = static_cast<PJ_DIRECTION>(-direction);
    switch (direction) {
    case PJ_FWD: {
        const auto xyz = pj_fwd3d(coo.lpz, P);
        coo.xyz = xyz;
        return coo;
    }
    case PJ_INV: {
        const auto lpz = pj_inv3d(coo.xyz, P);
        coo.lpz = lpz;
        return coo;
    }
    case PJ_IDENT:
        break;
    }
    return coo;
}

/**************************************************************************************/
int pj_has_inverse(PJ *P) {
    /***************************************************************************************
    Check if a a PJ has an inverse.
    ***************************************************************************************/
    return ((P->inverted && (P->fwd || P->fwd3d || P->fwd4d)) ||
            (P->inv || P->inv3d || P->inv4d));
}

/* Move P to a new context - or to the default context if 0 is specified */
void proj_context_set(PJ *P, PJ_CONTEXT *ctx) {
    if (nullptr == ctx)
        ctx = pj_get_default_ctx();
    proj_assign_context(P, ctx);
}

void proj_context_inherit(PJ *parent, PJ *child) {
    if (nullptr == parent)
        proj_assign_context(child, pj_get_default_ctx());
    else
        proj_assign_context(child, pj_get_ctx(parent));
}

/*****************************************************************************/
char *pj_chomp(char *c) {
    /******************************************************************************
    Strip pre- and postfix whitespace. Inline comments (indicated by '#') are
    considered whitespace.
    ******************************************************************************/
    size_t i, n;
    char *comment;
    char *start = c;

    if (nullptr == c)
        return nullptr;

    comment = strchr(c, '#');
    if (comment)
        *comment = 0;

    n = strlen(c);
    if (0 == n)
        return c;

    /* Eliminate postfix whitespace */
    for (i = n - 1; (i > 0) && (isspace(c[i]) || ';' == c[i]); i--)
        c[i] = 0;

    /* Find start of non-whitespace */
    while (0 != *start && (';' == *start || isspace(*start)))
        start++;

    n = strlen(start);
    if (0 == n) {
        c[0] = 0;
        return c;
    }

    memmove(c, start, n + 1);
    return c;
}

/*****************************************************************************/
char *pj_shrink(char *c) {
    /******************************************************************************
    Collapse repeated whitespace. Remove '+' and ';'. Make ',' and '=' greedy,
    consuming their surrounding whitespace.
    ******************************************************************************/
    size_t i, j, n;

    /* Flag showing that a whitespace (ws) has been written after last non-ws */
    bool ws = false;

    if (nullptr == c)
        return nullptr;

    pj_chomp(c);
    n = strlen(c);
    if (n == 0)
        return c;

    /* First collapse repeated whitespace (including +/;) */
    i = 0;
    bool in_string = false;
    for (j = 0; j < n; j++) {

        if (in_string) {
            if (c[j] == '"' && c[j + 1] == '"') {
                c[i++] = c[j];
                j++;
            } else if (c[j] == '"') {
                in_string = false;
            }
            c[i++] = c[j];
            continue;
        }

        /* Eliminate prefix '+', only if preceded by whitespace */
        /* (i.e. keep it in 1.23e+08) */
        if ((i > 0) && ('+' == c[j]) && ws)
            c[j] = ' ';
        if ((i == 0) && ('+' == c[j]))
            c[j] = ' ';

        // Detect a string beginning after '='
        if (c[j] == '"' && i > 0 && c[i - 1] == '=') {
            in_string = true;
            ws = false;
            c[i++] = c[j];
            continue;
        }

        if (isspace(c[j]) || ';' == c[j]) {
            if (false == ws && (i > 0))
                c[i++] = ' ';
            ws = true;
            continue;
        } else {
            ws = false;
            c[i++] = c[j];
        }
    }
    c[i] = 0;
    n = strlen(c);

    /* Then make ',' and '=' greedy */
    i = 0;
    for (j = 0; j < n; j++) {
        if (i == 0) {
            c[i++] = c[j];
            continue;
        }

        /* Skip space before '='/',' */
        if ('=' == c[j] || ',' == c[j]) {
            if (c[i - 1] == ' ')
                c[i - 1] = c[j];
            else
                c[i++] = c[j];
            continue;
        }

        if (' ' == c[j] && ('=' == c[i - 1] || ',' == c[i - 1]))
            continue;

        c[i++] = c[j];
    }
    c[i] = 0;
    return c;
}

/*****************************************************************************/
size_t pj_trim_argc(char *args) {
    /******************************************************************************
    Trim all unnecessary whitespace (and non-essential syntactic tokens) from
    the argument string, args, and count its number of elements.
    ******************************************************************************/
    size_t i, m, n;
    pj_shrink(args);
    n = strlen(args);
    if (n == 0)
        return 0;
    bool in_string = false;
    for (i = m = 0; i < n; i++) {
        if (in_string) {
            if (args[i] == '"' && args[i + 1] == '"') {
                i++;
            } else if (args[i] == '"') {
                in_string = false;
            }
        } else if (args[i] == '=' && args[i + 1] == '"') {
            i++;
            in_string = true;
        } else if (' ' == args[i]) {
            args[i] = 0;
            m++;
        }
    }
    return m + 1;
}

static void unquote_string(char *param_str) {

    size_t len = strlen(param_str);
    // Remove leading and terminating spaces after equal sign
    const char *equal = strstr(param_str, "=\"");
    if (equal && equal - param_str + 1 >= 2 && param_str[len - 1] == '"') {
        size_t dst = equal + 1 - param_str;
        size_t src = dst + 1;
        for (; param_str[src]; dst++, src++) {
            if (param_str[src] == '"') {
                if (param_str[src + 1] == '"') {
                    src++;
                } else {
                    break;
                }
            }
            param_str[dst] = param_str[src];
        }
        param_str[dst] = '\0';
    }
}

/*****************************************************************************/
char **pj_trim_argv(size_t argc, char *args) {
    /******************************************************************************
    Create an argv-style array from elements placed in the argument string,
    args.

    args is a trimmed string as returned by pj_trim_argc(), and argc is the
    number of trimmed strings found (i.e. the return value of pj_trim_args()).
    Hence, int argc    = pj_trim_argc (args); char **argv = pj_trim_argv (argc,
    args); will produce a classic style (argc, argv) pair from a string of
    whitespace separated args. No new memory is allocated for storing the
    individual args (they stay in the args string), but for the pointers to the
    args a new array is allocated and returned.

    It is the duty of the caller to free this array.
    ******************************************************************************/

    if (nullptr == args)
        return nullptr;
    if (0 == argc)
        return nullptr;

    /* turn the input string into an array of strings */
    char **argv = (char **)calloc(argc, sizeof(char *));
    if (nullptr == argv)
        return nullptr;
    for (size_t i = 0, j = 0; j < argc; j++) {
        argv[j] = args + i;
        char *str = argv[j];
        size_t nLen = strlen(str);
        i += nLen + 1;
        unquote_string(str);
    }
    return argv;
}

/*****************************************************************************/
std::string pj_double_quote_string_param_if_needed(const std::string &str) {
    /*****************************************************************************/
    if (str.find(' ') == std::string::npos) {
        return str;
    }
    std::string ret;
    ret += '"';
    ret += replaceAll(str, "\"", "\"\"");
    ret += '"';
    return ret;
}

/*****************************************************************************/
char *pj_make_args(size_t argc, char **argv) {
    /******************************************************************************
    pj_make_args is the inverse of the pj_trim_argc/pj_trim_argv combo: It
    converts free format command line input to something proj_create can
    consume.

    Allocates, and returns, an array of char, large enough to hold a whitespace
    separated copy of the args in argv. It is the duty of the caller to free
    this array.
    ******************************************************************************/
    try {
        std::string s;
        for (size_t i = 0; i < argc; i++) {
            const char *equal = strchr(argv[i], '=');
            if (equal) {
                s += std::string(argv[i], equal - argv[i] + 1);
                s += pj_double_quote_string_param_if_needed(equal + 1);
            } else {
                s += argv[i];
            }
            s += ' ';
        }

        char *p = pj_strdup(s.c_str());
        return pj_shrink(p);
    } catch (const std::exception &) {
        return nullptr;
    }
}

/*****************************************************************************/
void proj_context_errno_set(PJ_CONTEXT *ctx, int err) {
    /******************************************************************************
    Raise an error directly on a context, without going through a PJ belonging
    to that context.
    ******************************************************************************/
    if (nullptr == ctx)
        ctx = pj_get_default_ctx();
    ctx->last_errno = err;
    if (err == 0)
        return;
    errno = err;
}
