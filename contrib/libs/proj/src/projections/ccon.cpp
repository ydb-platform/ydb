/******************************************************************************
 * Copyright (c) 2017, Lukasz Komsta
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
#include <math.h>

#define EPS10 1e-10

namespace { // anonymous namespace
struct pj_ccon_data {
    double phi1;
    double ctgphi1;
    double sinphi1;
    double cosphi1;
    double *en;
};
} // anonymous namespace

PROJ_HEAD(ccon, "Central Conic")
"\n\tCentral Conic, Sph\n\tlat_1=";

static PJ_XY ccon_forward(PJ_LP lp, PJ *P) {
    PJ_XY xy = {0.0, 0.0};
    struct pj_ccon_data *Q = static_cast<struct pj_ccon_data *>(P->opaque);
    double r;

    r = Q->ctgphi1 - tan(lp.phi - Q->phi1);
    xy.x = r * sin(lp.lam * Q->sinphi1);
    xy.y = Q->ctgphi1 - r * cos(lp.lam * Q->sinphi1);

    return xy;
}

static PJ_LP ccon_inverse(PJ_XY xy, PJ *P) {
    PJ_LP lp = {0.0, 0.0};
    struct pj_ccon_data *Q = static_cast<struct pj_ccon_data *>(P->opaque);

    xy.y = Q->ctgphi1 - xy.y;
    lp.phi = Q->phi1 - atan(hypot(xy.x, xy.y) - Q->ctgphi1);
    lp.lam = atan2(xy.x, xy.y) / Q->sinphi1;

    return lp;
}

static PJ *pj_ccon_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_ccon_data *>(P->opaque)->en);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(ccon) {

    struct pj_ccon_data *Q = static_cast<struct pj_ccon_data *>(
        calloc(1, sizeof(struct pj_ccon_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_ccon_destructor;

    Q->phi1 = pj_param(P->ctx, P->params, "rlat_1").f;
    if (fabs(Q->phi1) < EPS10) {
        proj_log_error(P, _("Invalid value for lat_1: |lat_1| should be > 0"));
        return pj_ccon_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (!(Q->en = pj_enfn(P->n)))
        return pj_ccon_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    Q->sinphi1 = sin(Q->phi1);
    Q->cosphi1 = cos(Q->phi1);
    Q->ctgphi1 = Q->cosphi1 / Q->sinphi1;

    P->inv = ccon_inverse;
    P->fwd = ccon_forward;

    return P;
}

#undef EPS10
