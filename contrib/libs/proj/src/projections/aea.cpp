/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the aea (Albers Equal Area) projection.
 *           and the leac (Lambert Equal Area Conic) projection
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
#include <math.h>

#define EPS10 1.e-10
#define TOL7 1.e-7

PROJ_HEAD(aea, "Albers Equal Area") "\n\tConic Sph&Ell\n\tlat_1= lat_2=";
PROJ_HEAD(leac, "Lambert Equal Area Conic")
"\n\tConic, Sph&Ell\n\tlat_1= south";

namespace { // anonymous namespace
struct pj_aea {
    double ec;
    double n;
    double c;
    double dd;
    double n2;
    double rho0;
    double rho;
    double phi1;
    double phi2;
    int ellips;
    double *apa;
    double qp;
};
} // anonymous namespace

static PJ *pj_aea_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_aea *>(P->opaque)->apa);

    return pj_default_destructor(P, errlev);
}

static PJ_XY aea_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoid/spheroid, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_aea *Q = static_cast<struct pj_aea *>(P->opaque);
    Q->rho = Q->c - (Q->ellips ? Q->n * pj_authalic_lat_q(sin(lp.phi), P)
                               : Q->n2 * sin(lp.phi));
    if (Q->rho < 0.) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    Q->rho = Q->dd * sqrt(Q->rho);
    lp.lam *= Q->n;
    xy.x = Q->rho * sin(lp.lam);
    xy.y = Q->rho0 - Q->rho * cos(lp.lam);
    return xy;
}

static PJ_LP aea_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoid/spheroid, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_aea *Q = static_cast<struct pj_aea *>(P->opaque);
    xy.y = Q->rho0 - xy.y;
    Q->rho = hypot(xy.x, xy.y);
    if (Q->rho != 0.0) {
        if (Q->n < 0.) {
            Q->rho = -Q->rho;
            xy.x = -xy.x;
            xy.y = -xy.y;
        }
        lp.phi = Q->rho / Q->dd;
        if (Q->ellips) {
            const double qs = (Q->c - lp.phi * lp.phi) / Q->n;
            if (fabs(Q->ec - fabs(qs)) > TOL7) {
                if (fabs(qs) > 2) {
                    proj_errno_set(
                        P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                    return lp;
                }
                lp.phi =
                    pj_authalic_lat_inverse(asin(qs / Q->qp), Q->apa, P, Q->qp);
                if (lp.phi == HUGE_VAL) {
                    proj_errno_set(
                        P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                    return lp;
                }
            } else
                lp.phi = qs < 0. ? -M_HALFPI : M_HALFPI;
        } else {
            const double qs_div_2 = (Q->c - lp.phi * lp.phi) / Q->n2;
            if (fabs(qs_div_2) <= 1.)
                lp.phi = asin(qs_div_2);
            else
                lp.phi = qs_div_2 < 0. ? -M_HALFPI : M_HALFPI;
        }
        lp.lam = atan2(xy.x, xy.y) / Q->n;
    } else {
        lp.lam = 0.;
        lp.phi = Q->n > 0. ? M_HALFPI : -M_HALFPI;
    }
    return lp;
}

static PJ *setup(PJ *P) {
    struct pj_aea *Q = static_cast<struct pj_aea *>(P->opaque);

    P->inv = aea_e_inverse;
    P->fwd = aea_e_forward;

    if (fabs(Q->phi1) > M_HALFPI) {
        proj_log_error(P,
                       _("Invalid value for lat_1: |lat_1| should be <= 90°"));
        return pj_aea_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (fabs(Q->phi2) > M_HALFPI) {
        proj_log_error(P,
                       _("Invalid value for lat_2: |lat_2| should be <= 90°"));
        return pj_aea_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (fabs(Q->phi1 + Q->phi2) < EPS10) {
        proj_log_error(P, _("Invalid value for lat_1 and lat_2: |lat_1 + "
                            "lat_2| should be > 0"));
        return pj_aea_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    double sinphi = sin(Q->phi1);
    Q->n = sinphi;
    double cosphi = cos(Q->phi1);
    const int secant = fabs(Q->phi1 - Q->phi2) >= EPS10;
    Q->ellips = (P->es > 0.);
    if (Q->ellips) {
        double ml1, m1;

        Q->apa = pj_authalic_lat_compute_coeffs(P->n);
        if (Q->apa == nullptr)
            return pj_aea_destructor(P, 0);
        Q->qp = pj_authalic_lat_q(1.0, P);
        m1 = pj_msfn(sinphi, cosphi, P->es);
        ml1 = pj_authalic_lat_q(sinphi, P);
        if (secant) { /* secant cone */
            double ml2, m2;

            sinphi = sin(Q->phi2);
            cosphi = cos(Q->phi2);
            m2 = pj_msfn(sinphi, cosphi, P->es);
            ml2 = pj_authalic_lat_q(sinphi, P);
            if (ml2 == ml1)
                return pj_aea_destructor(P, 0);

            Q->n = (m1 * m1 - m2 * m2) / (ml2 - ml1);
            if (Q->n == 0) {
                // Not quite, but es is very close to 1...
                proj_log_error(P, _("Invalid value for eccentricity"));
                return pj_aea_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
        }
        Q->ec = 1. - .5 * P->one_es * log((1. - P->e) / (1. + P->e)) / P->e;
        Q->c = m1 * m1 + Q->n * ml1;
        Q->dd = 1. / Q->n;
        Q->rho0 =
            Q->dd * sqrt(Q->c - Q->n * pj_authalic_lat_q(sin(P->phi0), P));
    } else {
        if (secant)
            Q->n = .5 * (Q->n + sin(Q->phi2));
        Q->n2 = Q->n + Q->n;
        Q->c = cosphi * cosphi + Q->n2 * sinphi;
        Q->dd = 1. / Q->n;
        Q->rho0 = Q->dd * sqrt(Q->c - Q->n2 * sin(P->phi0));
    }

    return P;
}

PJ *PJ_PROJECTION(aea) {
    struct pj_aea *Q =
        static_cast<struct pj_aea *>(calloc(1, sizeof(struct pj_aea)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_aea_destructor;

    Q->phi1 = pj_param(P->ctx, P->params, "rlat_1").f;
    Q->phi2 = pj_param(P->ctx, P->params, "rlat_2").f;
    return setup(P);
}

PJ *PJ_PROJECTION(leac) {
    struct pj_aea *Q =
        static_cast<struct pj_aea *>(calloc(1, sizeof(struct pj_aea)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_aea_destructor;

    Q->phi2 = pj_param(P->ctx, P->params, "rlat_1").f;
    Q->phi1 = pj_param(P->ctx, P->params, "bsouth").i ? -M_HALFPI : M_HALFPI;
    return setup(P);
}

#undef EPS10
#undef TOL7
