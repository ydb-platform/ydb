/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the airy (Airy) projection.
 * Author:   Gerald Evenden (1995)
 *           Thomas Knudsen (2016) - revise/add regression tests
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

#include "proj.h"
#include "proj_internal.h"
#include <errno.h>

PROJ_HEAD(airy, "Airy") "\n\tMisc Sph, no inv\n\tno_cut lat_b=";

namespace { // anonymous namespace
enum Mode { N_POLE = 0, S_POLE = 1, EQUIT = 2, OBLIQ = 3 };
} // anonymous namespace

namespace { // anonymous namespace
struct pj_airy {
    double p_halfpi;
    double sinph0;
    double cosph0;
    double Cb;
    enum Mode mode;
    int no_cut; /* do not cut at hemisphere limit */
};
} // anonymous namespace

#define EPS 1.e-10

static PJ_XY airy_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_airy *Q = static_cast<struct pj_airy *>(P->opaque);
    double sinlam, coslam, cosphi, sinphi, t, s, Krho, cosz;

    sinlam = sin(lp.lam);
    coslam = cos(lp.lam);
    switch (Q->mode) {
    case EQUIT:
    case OBLIQ:
        sinphi = sin(lp.phi);
        cosphi = cos(lp.phi);
        cosz = cosphi * coslam;
        if (Q->mode == OBLIQ)
            cosz = Q->sinph0 * sinphi + Q->cosph0 * cosz;
        if (!Q->no_cut && cosz < -EPS) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        s = 1. - cosz;
        if (fabs(s) > EPS) {
            t = 0.5 * (1. + cosz);
            if (t == 0) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            Krho = -log(t) / s - Q->Cb / t;
        } else
            Krho = 0.5 - Q->Cb;
        xy.x = Krho * cosphi * sinlam;
        if (Q->mode == OBLIQ)
            xy.y = Krho * (Q->cosph0 * sinphi - Q->sinph0 * cosphi * coslam);
        else
            xy.y = Krho * sinphi;
        break;
    case S_POLE:
    case N_POLE:
        lp.phi = fabs(Q->p_halfpi - lp.phi);
        if (!Q->no_cut && (lp.phi - EPS) > M_HALFPI) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        lp.phi *= 0.5;
        if (lp.phi > EPS) {
            t = tan(lp.phi);
            Krho = -2. * (log(cos(lp.phi)) / t + t * Q->Cb);
            xy.x = Krho * sinlam;
            xy.y = Krho * coslam;
            if (Q->mode == N_POLE)
                xy.y = -xy.y;
        } else
            xy.x = xy.y = 0.;
    }
    return xy;
}

PJ *PJ_PROJECTION(airy) {
    double beta;

    struct pj_airy *Q =
        static_cast<struct pj_airy *>(calloc(1, sizeof(struct pj_airy)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    P->opaque = Q;

    Q->no_cut = pj_param(P->ctx, P->params, "bno_cut").i;
    beta = 0.5 * (M_HALFPI - pj_param(P->ctx, P->params, "rlat_b").f);
    if (fabs(beta) < EPS)
        Q->Cb = -0.5;
    else {
        Q->Cb = 1. / tan(beta);
        Q->Cb *= Q->Cb * log(cos(beta));
    }

    if (fabs(fabs(P->phi0) - M_HALFPI) < EPS)
        if (P->phi0 < 0.) {
            Q->p_halfpi = -M_HALFPI;
            Q->mode = S_POLE;
        } else {
            Q->p_halfpi = M_HALFPI;
            Q->mode = N_POLE;
        }
    else {
        if (fabs(P->phi0) < EPS)
            Q->mode = EQUIT;
        else {
            Q->mode = OBLIQ;
            Q->sinph0 = sin(P->phi0);
            Q->cosph0 = cos(P->phi0);
        }
    }
    P->fwd = airy_s_forward;
    P->es = 0.;
    return P;
}

#undef EPS
