/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Forward operation invocation
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2018-01-02
 *           Based on material from Gerald Evenden (original pj_fwd)
 *           and Piyush Agram (original pj_fwd3d)
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

#define INPUT_UNITS P->left
#define OUTPUT_UNITS P->right

static void fwd_prepare(PJ *P, PJ_COORD &coo) {
    if (HUGE_VAL == coo.v[0] || HUGE_VAL == coo.v[1] || HUGE_VAL == coo.v[2]) {
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

    /* Check validity of angular input coordinates */
    if (INPUT_UNITS == PJ_IO_UNITS_RADIANS) {
        double t;

        /* check for latitude or longitude over-range */
        t = (coo.lp.phi < 0 ? -coo.lp.phi : coo.lp.phi) - M_HALFPI;
        if (t > PJ_EPS_LAT) {
            proj_log_error(P, _("Invalid latitude"));
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_INVALID_COORD);
            coo = proj_coord_error();
            return;
        }
        if (coo.lp.lam > 10 || coo.lp.lam < -10) {
            proj_log_error(P, _("Invalid longitude"));
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_INVALID_COORD);
            coo = proj_coord_error();
            return;
        }

        /* Clamp latitude to -90..90 degree range */
        if (coo.lp.phi > M_HALFPI)
            coo.lp.phi = M_HALFPI;
        if (coo.lp.phi < -M_HALFPI)
            coo.lp.phi = -M_HALFPI;

        /* If input latitude is geocentrical, convert to geographical */
        if (P->geoc)
            coo = pj_geocentric_latitude(P, PJ_INV, coo);

        /* Ensure longitude is in the -pi:pi range */
        if (0 == P->over)
            coo.lp.lam = adjlon(coo.lp.lam);

        if (P->hgridshift)
            coo = proj_trans(P->hgridshift, PJ_INV, coo);
        else if (P->helmert ||
                 (P->cart_wgs84 != nullptr && P->cart != nullptr)) {
            coo = proj_trans(P->cart_wgs84, PJ_FWD,
                             coo); /* Go cartesian in WGS84 frame */
            if (P->helmert)
                coo = proj_trans(P->helmert, PJ_INV,
                                 coo); /* Step into local frame */
            coo = proj_trans(P->cart, PJ_INV,
                             coo); /* Go back to angular using local ellps */
        }
        if (coo.lp.lam == HUGE_VAL)
            return;
        if (P->vgridshift)
            coo = proj_trans(P->vgridshift, PJ_FWD,
                             coo); /* Go orthometric from geometric */

        /* Distance from central meridian, taking system zero meridian into
         * account
         */
        coo.lp.lam = (coo.lp.lam - P->from_greenwich) - P->lam0;

        /* Ensure longitude is in the -pi:pi range */
        if (0 == P->over)
            coo.lp.lam = adjlon(coo.lp.lam);

        return;
    }

    /* We do not support gridshifts on cartesian input */
    if (INPUT_UNITS == PJ_IO_UNITS_CARTESIAN && P->helmert)
        coo = proj_trans(P->helmert, PJ_INV, coo);
    return;
}

static void fwd_finalize(PJ *P, PJ_COORD &coo) {

    switch (OUTPUT_UNITS) {

    /* Handle false eastings/northings and non-metric linear units */
    case PJ_IO_UNITS_CARTESIAN:

        if (P->is_geocent) {
            coo = proj_trans(P->cart, PJ_FWD, coo);
        }
        coo.xyz.x *= P->fr_meter;
        coo.xyz.y *= P->fr_meter;
        coo.xyz.z *= P->fr_meter;

        break;

    /* Classic proj.4 functions return plane coordinates in units of the
     * semimajor axis */
    case PJ_IO_UNITS_CLASSIC:
        coo.xy.x *= P->a;
        coo.xy.y *= P->a;
        PROJ_FALLTHROUGH;

    /* to continue processing in common with PJ_IO_UNITS_PROJECTED */
    case PJ_IO_UNITS_PROJECTED:
        coo.xyz.x = P->fr_meter * (coo.xyz.x + P->x0);
        coo.xyz.y = P->fr_meter * (coo.xyz.y + P->y0);
        coo.xyz.z = P->vfr_meter * (coo.xyz.z + P->z0);
        break;

    case PJ_IO_UNITS_WHATEVER:
        break;

    case PJ_IO_UNITS_DEGREES:
        break;

    case PJ_IO_UNITS_RADIANS:
        coo.lpz.z = P->vfr_meter * (coo.lpz.z + P->z0);

        if (P->is_long_wrap_set) {
            if (coo.lpz.lam != HUGE_VAL) {
                coo.lpz.lam = P->long_wrap_center +
                              adjlon(coo.lpz.lam - P->long_wrap_center);
            }
        }

        break;
    }

    if (P->axisswap)
        coo = proj_trans(P->axisswap, PJ_FWD, coo);
}

static inline PJ_COORD error_or_coord(PJ *P, PJ_COORD coord, int last_errno) {
    if (P->ctx->last_errno)
        return proj_coord_error();

    P->ctx->last_errno = last_errno;

    return coord;
}

PJ_XY pj_fwd(PJ_LP lp, PJ *P) {
    PJ_COORD coo = {{0, 0, 0, 0}};
    coo.lp = lp;

    const int last_errno = P->ctx->last_errno;
    P->ctx->last_errno = 0;

    if (!P->skip_fwd_prepare)
        fwd_prepare(P, coo);
    if (HUGE_VAL == coo.v[0] || HUGE_VAL == coo.v[1])
        return proj_coord_error().xy;

    /* Do the transformation, using the lowest dimensional transformer available
     */
    if (P->fwd) {
        const auto xy = P->fwd(coo.lp, P);
        coo.xy = xy;
    } else if (P->fwd3d) {
        const auto xyz = P->fwd3d(coo.lpz, P);
        coo.xyz = xyz;
    } else if (P->fwd4d)
        P->fwd4d(coo, P);
    else {
        proj_errno_set(P, PROJ_ERR_OTHER_NO_INVERSE_OP);
        return proj_coord_error().xy;
    }
    if (HUGE_VAL == coo.v[0])
        return proj_coord_error().xy;

    if (!P->skip_fwd_finalize)
        fwd_finalize(P, coo);

    return error_or_coord(P, coo, last_errno).xy;
}

PJ_XYZ pj_fwd3d(PJ_LPZ lpz, PJ *P) {
    PJ_COORD coo = {{0, 0, 0, 0}};
    coo.lpz = lpz;

    const int last_errno = P->ctx->last_errno;
    P->ctx->last_errno = 0;

    if (!P->skip_fwd_prepare)
        fwd_prepare(P, coo);
    if (HUGE_VAL == coo.v[0])
        return proj_coord_error().xyz;

    /* Do the transformation, using the lowest dimensional transformer feasible
     */
    if (P->fwd3d) {
        const auto xyz = P->fwd3d(coo.lpz, P);
        coo.xyz = xyz;
    } else if (P->fwd4d)
        P->fwd4d(coo, P);
    else if (P->fwd) {
        const auto xy = P->fwd(coo.lp, P);
        coo.xy = xy;
    } else {
        proj_errno_set(P, PROJ_ERR_OTHER_NO_INVERSE_OP);
        return proj_coord_error().xyz;
    }
    if (HUGE_VAL == coo.v[0])
        return proj_coord_error().xyz;

    if (!P->skip_fwd_finalize)
        fwd_finalize(P, coo);

    return error_or_coord(P, coo, last_errno).xyz;
}

bool pj_fwd4d(PJ_COORD &coo, PJ *P) {

    const int last_errno = P->ctx->last_errno;
    P->ctx->last_errno = 0;

    if (!P->skip_fwd_prepare)
        fwd_prepare(P, coo);
    if (HUGE_VAL == coo.v[0]) {
        coo = proj_coord_error();
        return false;
    }

    /* Call the highest dimensional converter available */
    if (P->fwd4d)
        P->fwd4d(coo, P);
    else if (P->fwd3d) {
        const auto xyz = P->fwd3d(coo.lpz, P);
        coo.xyz = xyz;
    } else if (P->fwd) {
        const auto xy = P->fwd(coo.lp, P);
        coo.xy = xy;
    } else {
        proj_errno_set(P, PROJ_ERR_OTHER_NO_INVERSE_OP);
        coo = proj_coord_error();
        return false;
    }
    if (HUGE_VAL == coo.v[0]) {
        coo = proj_coord_error();
        return false;
    }

    if (!P->skip_fwd_finalize)
        fwd_finalize(P, coo);

    if (P->ctx->last_errno) {
        coo = proj_coord_error();
        return false;
    }

    P->ctx->last_errno = last_errno;
    return true;
}
