/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Inverse operation invocation
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2018-01-02
 *           Based on material from Gerald Evenden (original pj_inv)
 *           and Piyush Agram (original pj_inv3d)
 *
 ******************************************************************************
 * Copyright (c) 2000, Frank Warmerdam
 * Copyright (c) 2018, Thomas Knudsen / SDFE
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
#include <errno.h>
#include <math.h>

#include "proj_internal.h"
#include <math.h>

#define INPUT_UNITS P->right
#define OUTPUT_UNITS P->left

static void inv_prepare(PJ *P, PJ_COORD &coo) {
    if (coo.v[0] == HUGE_VAL || coo.v[1] == HUGE_VAL || coo.v[2] == HUGE_VAL) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        coo = proj_coord_error();
        return;
    }

    /* The helmert datum shift will choke unless it gets a sensible 4D
     * coordinate
     */
    if (HUGE_VAL == coo.v[2] && P->helmert)
        coo.v[2] = 0.0;
    if (HUGE_VAL == coo.v[3] && P->helmert)
        coo.v[3] = 0.0;

    if (P->axisswap)
        coo = proj_trans(P->axisswap, PJ_INV, coo);

    /* Handle remaining possible input types */
    switch (INPUT_UNITS) {
    case PJ_IO_UNITS_WHATEVER:
        break;

    case PJ_IO_UNITS_DEGREES:
        break;

    /* de-scale and de-offset */
    case PJ_IO_UNITS_CARTESIAN:
        coo.xyz.x *= P->to_meter;
        coo.xyz.y *= P->to_meter;
        coo.xyz.z *= P->to_meter;
        if (P->is_geocent) {
            coo = proj_trans(P->cart, PJ_INV, coo);
        }
        break;

    case PJ_IO_UNITS_PROJECTED:
    case PJ_IO_UNITS_CLASSIC:
        coo.xyz.x = P->to_meter * coo.xyz.x - P->x0;
        coo.xyz.y = P->to_meter * coo.xyz.y - P->y0;
        coo.xyz.z = P->vto_meter * coo.xyz.z - P->z0;
        if (INPUT_UNITS == PJ_IO_UNITS_PROJECTED)
            return;

        /* Classic proj.4 functions expect plane coordinates in units of the
         * semimajor axis  */
        /* Multiplying by ra, rather than dividing by a because the CalCOFI
         * projection       */
        /* stomps on a and hence (apparently) depends on this to roundtrip
         * correctly
         */
        /* (CalCOFI avoids further scaling by stomping - but a better solution
         * is possible)  */
        coo.xyz.x *= P->ra;
        coo.xyz.y *= P->ra;
        break;

    case PJ_IO_UNITS_RADIANS:
        coo.lpz.z = P->vto_meter * coo.lpz.z - P->z0;
        break;
    }
}

static void inv_finalize(PJ *P, PJ_COORD &coo) {
    if (coo.xyz.x == HUGE_VAL) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        coo = proj_coord_error();
    }

    if (OUTPUT_UNITS == PJ_IO_UNITS_RADIANS) {

        /* Distance from central meridian, taking system zero meridian into
         * account
         */
        coo.lp.lam = coo.lp.lam + P->from_greenwich + P->lam0;

        /* adjust longitude to central meridian */
        if (0 == P->over)
            coo.lpz.lam = adjlon(coo.lpz.lam);

        if (P->vgridshift)
            coo = proj_trans(P->vgridshift, PJ_INV,
                             coo); /* Go geometric from orthometric */
        if (coo.lp.lam == HUGE_VAL)
            return;
        if (P->hgridshift)
            coo = proj_trans(P->hgridshift, PJ_FWD, coo);
        else if (P->helmert ||
                 (P->cart_wgs84 != nullptr && P->cart != nullptr)) {
            coo = proj_trans(P->cart, PJ_FWD,
                             coo); /* Go cartesian in local frame */
            if (P->helmert)
                coo = proj_trans(P->helmert, PJ_FWD, coo); /* Step into WGS84 */
            coo = proj_trans(P->cart_wgs84, PJ_INV,
                             coo); /* Go back to angular using WGS84 ellps */
        }
        if (coo.lp.lam == HUGE_VAL)
            return;

        /* If input latitude was geocentrical, convert back to geocentrical */
        if (P->geoc)
            coo = pj_geocentric_latitude(P, PJ_FWD, coo);
    }
}

static inline PJ_COORD error_or_coord(PJ *P, PJ_COORD coord, int last_errno) {
    if (P->ctx->last_errno)
        return proj_coord_error();

    P->ctx->last_errno = last_errno;

    return coord;
}

PJ_LP pj_inv(PJ_XY xy, PJ *P) {
    PJ_COORD coo = {{0, 0, 0, 0}};
    coo.xy = xy;

    const int last_errno = P->ctx->last_errno;
    P->ctx->last_errno = 0;

    if (!P->skip_inv_prepare)
        inv_prepare(P, coo);
    if (HUGE_VAL == coo.v[0])
        return proj_coord_error().lp;

    /* Do the transformation, using the lowest dimensional transformer available
     */
    if (P->inv) {
        const auto lp = P->inv(coo.xy, P);
        coo.lp = lp;
    } else if (P->inv3d) {
        const auto lpz = P->inv3d(coo.xyz, P);
        coo.lpz = lpz;
    } else if (P->inv4d)
        P->inv4d(coo, P);
    else {
        proj_errno_set(P, PROJ_ERR_OTHER_NO_INVERSE_OP);
        return proj_coord_error().lp;
    }
    if (HUGE_VAL == coo.v[0])
        return proj_coord_error().lp;

    if (!P->skip_inv_finalize)
        inv_finalize(P, coo);

    return error_or_coord(P, coo, last_errno).lp;
}

PJ_LPZ pj_inv3d(PJ_XYZ xyz, PJ *P) {
    PJ_COORD coo = {{0, 0, 0, 0}};
    coo.xyz = xyz;

    const int last_errno = P->ctx->last_errno;
    P->ctx->last_errno = 0;

    if (!P->skip_inv_prepare)
        inv_prepare(P, coo);
    if (HUGE_VAL == coo.v[0])
        return proj_coord_error().lpz;

    /* Do the transformation, using the lowest dimensional transformer feasible
     */
    if (P->inv3d) {
        const auto lpz = P->inv3d(coo.xyz, P);
        coo.lpz = lpz;
    } else if (P->inv4d)
        P->inv4d(coo, P);
    else if (P->inv) {
        const auto lp = P->inv(coo.xy, P);
        coo.lp = lp;
    } else {
        proj_errno_set(P, PROJ_ERR_OTHER_NO_INVERSE_OP);
        return proj_coord_error().lpz;
    }
    if (HUGE_VAL == coo.v[0])
        return proj_coord_error().lpz;

    if (!P->skip_inv_finalize)
        inv_finalize(P, coo);

    return error_or_coord(P, coo, last_errno).lpz;
}

bool pj_inv4d(PJ_COORD &coo, PJ *P) {

    const int last_errno = P->ctx->last_errno;
    P->ctx->last_errno = 0;

    if (!P->skip_inv_prepare)
        inv_prepare(P, coo);
    if (HUGE_VAL == coo.v[0]) {
        coo = proj_coord_error();
        return false;
    }

    /* Call the highest dimensional converter available */
    if (P->inv4d)
        P->inv4d(coo, P);
    else if (P->inv3d) {
        const auto lpz = P->inv3d(coo.xyz, P);
        coo.lpz = lpz;
    } else if (P->inv) {
        const auto lp = P->inv(coo.xy, P);
        coo.lp = lp;
    } else {
        proj_errno_set(P, PROJ_ERR_OTHER_NO_INVERSE_OP);
        coo = proj_coord_error();
        return false;
    }
    if (HUGE_VAL == coo.v[0]) {
        coo = proj_coord_error();
        return false;
    }

    if (!P->skip_inv_finalize)
        inv_finalize(P, coo);

    if (P->ctx->last_errno) {
        coo = proj_coord_error();
        return false;
    }

    P->ctx->last_errno = last_errno;
    return true;
}
