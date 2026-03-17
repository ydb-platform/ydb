/*
** libproj -- library of cartographic projections
**
** Copyright (c) 2003   Gerald I. Evenden
*/
/*
** Permission is hereby granted, free of charge, to any person obtaining
** a copy of this software and associated documentation files (the
** "Software"), to deal in the Software without restriction, including
** without limitation the rights to use, copy, modify, merge, publish,
** distribute, sublicense, and/or sell copies of the Software, and to
** permit persons to whom the Software is furnished to do so, subject to
** the following conditions:
**
** The above copyright notice and this permission notice shall be
** included in all copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
** EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
** MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
** IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
** CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
** TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
** SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

namespace { // anonymous namespace
struct pj_opaque {
    double phic0;
    double cosc0, sinc0;
    double R2;
    void *en;
};
} // anonymous namespace

PROJ_HEAD(sterea, "Oblique Stereographic Alternative") "\n\tAzimuthal, Sph&Ell";

static PJ_XY sterea_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque *>(P->opaque);
    double cosc, sinc, cosl, k;

    lp = pj_gauss(P->ctx, lp, Q->en);
    sinc = sin(lp.phi);
    cosc = cos(lp.phi);
    cosl = cos(lp.lam);
    const double denom = 1. + Q->sinc0 * sinc + Q->cosc0 * cosc * cosl;
    if (denom == 0.0) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().xy;
    }
    k = P->k0 * Q->R2 / denom;
    xy.x = k * cosc * sin(lp.lam);
    xy.y = k * (Q->cosc0 * sinc - Q->sinc0 * cosc * cosl);
    return xy;
}

static PJ_LP sterea_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque *>(P->opaque);
    double rho, c, sinc, cosc;

    xy.x /= P->k0;
    xy.y /= P->k0;
    if ((rho = hypot(xy.x, xy.y)) != 0.0) {
        c = 2. * atan2(rho, Q->R2);
        sinc = sin(c);
        cosc = cos(c);
        lp.phi = asin(cosc * Q->sinc0 + xy.y * sinc * Q->cosc0 / rho);
        lp.lam =
            atan2(xy.x * sinc, rho * Q->cosc0 * cosc - xy.y * Q->sinc0 * sinc);
    } else {
        lp.phi = Q->phic0;
        lp.lam = 0.;
    }
    return pj_inv_gauss(P->ctx, lp, Q->en);
}

static PJ *destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_opaque *>(P->opaque)->en);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(sterea) {
    double R;
    struct pj_opaque *Q =
        static_cast<struct pj_opaque *>(calloc(1, sizeof(struct pj_opaque)));

    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->en = pj_gauss_ini(P->e, P->phi0, &(Q->phic0), &R);
    if (nullptr == Q->en)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    Q->sinc0 = sin(Q->phic0);
    Q->cosc0 = cos(Q->phic0);
    Q->R2 = 2. * R;

    P->inv = sterea_e_inverse;
    P->fwd = sterea_e_forward;
    P->destructor = destructor;

    return P;
}
