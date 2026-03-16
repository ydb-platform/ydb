/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the nzmg (New Zealand Map Grid) projection.
 *           Very loosely based upon DMA code by Bradford W. Drew
 * Author:   Gerald Evenden
 *
 ******************************************************************************
 * Copyright (c) 1995, Gerald Evenden
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

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(nzmg, "New Zealand Map Grid") "\n\tfixed Earth";

#define EPSLN 1e-10
#define SEC5_TO_RAD 0.4848136811095359935899141023
#define RAD_TO_SEC5 2.062648062470963551564733573

static const COMPLEX bf[] = {
    {.7557853228, 0.0},         {.249204646, 0.003371507},
    {-.001541739, 0.041058560}, {-.10162907, 0.01727609},
    {-.26623489, -0.36249218},  {-.6870983, -1.1651967}};

#define Nbf 5
#define Ntpsi 9
#define Ntphi 8

static PJ_XY nzmg_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    COMPLEX p;
    const double *C;
    int i;

    static const double tpsi[] = {
        .6399175073, -.1358797613, .063294409, -.02526853, .0117879,
        -.0055161,   .0026906,     -.001333,   .00067,     -.00034};

    lp.phi = (lp.phi - P->phi0) * RAD_TO_SEC5;
    i = Ntpsi;
    C = tpsi + i;
    for (p.r = *C; i; --i) {
        --C;
        p.r = *C + lp.phi * p.r;
    }
    p.r *= lp.phi;
    p.i = lp.lam;
    p = pj_zpoly1(p, bf, Nbf);
    xy.x = p.i;
    xy.y = p.r;

    return xy;
}

static PJ_LP nzmg_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    int nn, i;
    COMPLEX p, f, fp, dp;
    double den;
    const double *C;

    static const double tphi[] = {1.5627014243, .5185406398, -.03333098,
                                  -.1052906,    -.0368594,   .007317,
                                  .01220,       .00394,      -.0013};

    p.r = xy.y;
    p.i = xy.x;
    for (nn = 20; nn; --nn) {
        f = pj_zpolyd1(p, bf, Nbf, &fp);
        f.r -= xy.y;
        f.i -= xy.x;
        den = fp.r * fp.r + fp.i * fp.i;
        dp.r = -(f.r * fp.r + f.i * fp.i) / den;
        dp.i = -(f.i * fp.r - f.r * fp.i) / den;
        p.r += dp.r;
        p.i += dp.i;
        if ((fabs(dp.r) + fabs(dp.i)) <= EPSLN)
            break;
    }
    if (nn) {
        lp.lam = p.i;
        i = Ntphi;
        C = tphi + i;
        for (lp.phi = *C; i; --i) {
            --C;
            lp.phi = *C + p.r * lp.phi;
        }
        lp.phi = P->phi0 + p.r * lp.phi * SEC5_TO_RAD;
    } else
        lp.lam = lp.phi = HUGE_VAL;

    return lp;
}

PJ *PJ_PROJECTION(nzmg) {
    /* force to International major axis */
    P->ra = 1. / (P->a = 6378388.0);
    P->lam0 = DEG_TO_RAD * 173.;
    P->phi0 = DEG_TO_RAD * -41.;
    P->x0 = 2510000.;
    P->y0 = 6023150.;

    P->inv = nzmg_e_inverse;
    P->fwd = nzmg_e_forward;

    return P;
}

#undef EPSLN
#undef SEC5_TO_RAD
#undef RAD_TO_SEC5
#undef Nbf
#undef Ntpsi
#undef Ntphi
