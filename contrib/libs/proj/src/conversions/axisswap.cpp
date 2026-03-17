/***********************************************************************

        Axis order operation for use with transformation pipelines.

                Kristian Evers, kreve@sdfe.dk, 2017-10-31

************************************************************************

Change the order and sign of 2,3 or 4 axes. Each of the possible four
axes are numbered with 1-4, such that the first input axis is 1, the
second is 2 and so on. The output ordering is controlled by a list of the
input axes re-ordered to the new mapping. Examples:

Reversing the order of the axes:

    +proj=axisswap +order=4,3,2,1

Swapping the first two axes (x and y):

    +proj=axisswap +order=2,1,3,4

The direction, or sign, of an axis can be changed by adding a minus in
front of the axis-number:

    +proj=axisswap +order=1,-2,3,4

It is only necessary to specify the axes that are affected by the swap
operation:

    +proj=axisswap +order=2,1

************************************************************************
* Copyright (c) 2017, Kristian Evers / SDFE
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
*
***********************************************************************/

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(axisswap, "Axis ordering");

namespace { // anonymous namespace
struct pj_axisswap_data {
    unsigned int axis[4];
    int sign[4];
};
} // anonymous namespace

static int sign(int x) { return (x > 0) - (x < 0); }

static PJ_XY pj_axisswap_forward_2d(PJ_LP lp, PJ *P) {
    struct pj_axisswap_data *Q = (struct pj_axisswap_data *)P->opaque;
    PJ_XY xy;

    double in[2] = {lp.lam, lp.phi};
    xy.x = in[Q->axis[0]] * Q->sign[0];
    xy.y = in[Q->axis[1]] * Q->sign[1];
    return xy;
}

static PJ_LP pj_axisswap_reverse_2d(PJ_XY xy, PJ *P) {
    struct pj_axisswap_data *Q = (struct pj_axisswap_data *)P->opaque;
    unsigned int i;
    PJ_COORD out, in;

    in.v[0] = xy.x;
    in.v[1] = xy.y;
    out = proj_coord_error();

    for (i = 0; i < 2; i++)
        out.v[Q->axis[i]] = in.v[i] * Q->sign[i];

    return out.lp;
}

static PJ_XYZ pj_axisswap_forward_3d(PJ_LPZ lpz, PJ *P) {
    struct pj_axisswap_data *Q = (struct pj_axisswap_data *)P->opaque;
    unsigned int i;
    PJ_COORD out, in;

    in.v[0] = lpz.lam;
    in.v[1] = lpz.phi;
    in.v[2] = lpz.z;
    out = proj_coord_error();

    for (i = 0; i < 3; i++)
        out.v[i] = in.v[Q->axis[i]] * Q->sign[i];

    return out.xyz;
}

static PJ_LPZ pj_axisswap_reverse_3d(PJ_XYZ xyz, PJ *P) {
    struct pj_axisswap_data *Q = (struct pj_axisswap_data *)P->opaque;
    unsigned int i;
    PJ_COORD in, out;

    out = proj_coord_error();
    in.v[0] = xyz.x;
    in.v[1] = xyz.y;
    in.v[2] = xyz.z;

    for (i = 0; i < 3; i++)
        out.v[Q->axis[i]] = in.v[i] * Q->sign[i];

    return out.lpz;
}

static void swap_xy_4d(PJ_COORD &coo, PJ *) {
    std::swap(coo.xyzt.x, coo.xyzt.y);
}

static void pj_axisswap_forward_4d(PJ_COORD &coo, PJ *P) {
    struct pj_axisswap_data *Q = (struct pj_axisswap_data *)P->opaque;
    unsigned int i;
    PJ_COORD out;

    for (i = 0; i < 4; i++)
        out.v[i] = coo.v[Q->axis[i]] * Q->sign[i];
    coo = out;
}

static void pj_axisswap_reverse_4d(PJ_COORD &coo, PJ *P) {
    struct pj_axisswap_data *Q = (struct pj_axisswap_data *)P->opaque;
    unsigned int i;
    PJ_COORD out;

    for (i = 0; i < 4; i++)
        out.v[Q->axis[i]] = coo.v[i] * Q->sign[i];

    coo = out;
}

/***********************************************************************/
PJ *PJ_CONVERSION(axisswap, 0) {
    /***********************************************************************/
    struct pj_axisswap_data *Q = static_cast<struct pj_axisswap_data *>(
        calloc(1, sizeof(struct pj_axisswap_data)));
    char *s;
    unsigned int i, j, n = 0;

    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = (void *)Q;

    /* +order and +axis are mutually exclusive */
    if (!pj_param_exists(P->params, "order") ==
        !pj_param_exists(P->params, "axis")) {
        proj_log_error(P,
                       _("must provide EITHER 'order' OR 'axis' parameter."));
        return pj_default_destructor(
            P, PROJ_ERR_INVALID_OP_MUTUALLY_EXCLUSIVE_ARGS);
    }

    /* fill axis list with indices from 4-7 to simplify duplicate search further
     * down */
    for (i = 0; i < 4; i++) {
        Q->axis[i] = i + 4;
        Q->sign[i] = 1;
    }

    /* if the "order" parameter is used */
    if (pj_param_exists(P->params, "order")) {
        /* read axis order */
        char *order = pj_param(P->ctx, P->params, "sorder").s;

        /* check that all characters are valid */
        for (i = 0; i < strlen(order); i++)
            if (strchr("1234-,", order[i]) == nullptr) {
                proj_log_error(P, _("unknown axis '%c'"), order[i]);
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }

        /* read axes numbers and signs */
        s = order;
        n = 0;
        while (*s != '\0' && n < 4) {
            Q->axis[n] = abs(atoi(s)) - 1;
            if (Q->axis[n] > 3) {
                proj_log_error(P, _("invalid axis '%d'"), Q->axis[n]);
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
            Q->sign[n++] = sign(atoi(s));
            while (*s != '\0' && *s != ',')
                s++;
            if (*s == ',')
                s++;
        }
    }

    /* if the "axis" parameter is used */
    if (pj_param_exists(P->params, "axis")) {
        /* parse the classic PROJ.4 enu axis specification */
        for (i = 0; i < 3; i++) {
            switch (P->axis[i]) {
            case 'w':
                Q->sign[i] = -1;
                Q->axis[i] = 0;
                break;
            case 'e':
                Q->sign[i] = 1;
                Q->axis[i] = 0;
                break;
            case 's':
                Q->sign[i] = -1;
                Q->axis[i] = 1;
                break;
            case 'n':
                Q->sign[i] = 1;
                Q->axis[i] = 1;
                break;
            case 'd':
                Q->sign[i] = -1;
                Q->axis[i] = 2;
                break;
            case 'u':
                Q->sign[i] = 1;
                Q->axis[i] = 2;
                break;
            default:
                proj_log_error(P, _("unknown axis '%c'"), P->axis[i]);
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
        }
        n = 3;
    }

    /* check for duplicate axes */
    for (i = 0; i < 4; i++)
        for (j = 0; j < 4; j++) {
            if (i == j)
                continue;
            if (Q->axis[i] == Q->axis[j]) {
                proj_log_error(P, _("axisswap: duplicate axes specified"));
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
        }

    /* only map fwd/inv functions that are possible with the given axis setup */
    if (n == 4) {
        P->fwd4d = pj_axisswap_forward_4d;
        P->inv4d = pj_axisswap_reverse_4d;
    }
    if (n == 3 && Q->axis[0] < 3 && Q->axis[1] < 3 && Q->axis[2] < 3) {
        P->fwd3d = pj_axisswap_forward_3d;
        P->inv3d = pj_axisswap_reverse_3d;
    }
    if (n == 2) {
        if (Q->axis[0] == 1 && Q->sign[0] == 1 && Q->axis[1] == 0 &&
            Q->sign[1] == 1) {
            P->fwd4d = swap_xy_4d;
            P->inv4d = swap_xy_4d;
        } else if (Q->axis[0] < 2 && Q->axis[1] < 2) {
            P->fwd = pj_axisswap_forward_2d;
            P->inv = pj_axisswap_reverse_2d;
        }
    }

    if (P->fwd4d == nullptr && P->fwd3d == nullptr && P->fwd == nullptr) {
        proj_log_error(P, _("axisswap: bad axis order"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    if (pj_param(P->ctx, P->params, "tangularunits").i) {
        P->left = PJ_IO_UNITS_RADIANS;
        P->right = PJ_IO_UNITS_RADIANS;
    } else {
        P->left = PJ_IO_UNITS_WHATEVER;
        P->right = PJ_IO_UNITS_WHATEVER;
    }

    /* Preparation and finalization steps are skipped, since the reason   */
    /* d'etre of axisswap is to bring input coordinates in line with the  */
    /* the internally expected order (ENU), such that handling of offsets */
    /* etc. can be done correctly in a later step of a pipeline */
    P->skip_fwd_prepare = 1;
    P->skip_fwd_finalize = 1;
    P->skip_inv_prepare = 1;
    P->skip_inv_finalize = 1;

    return P;
}
