/*
** Copyright (c) 2003, 2006   Gerald I. Evenden
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

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(omerc, "Oblique Mercator")
"\n\tCyl, Sph&Ell no_rot\n\t"
    "alpha= [gamma=] [no_off] lonc= or\n\t lon_1= lat_1= lon_2= lat_2=";

namespace { // anonymous namespace
struct pj_omerc_data {
    double A, B, E, AB, ArB, BrA, rB, singam, cosgam, sinrot, cosrot;
    double v_pole_n, v_pole_s, u_0;
    int no_rot;
};
} // anonymous namespace

#define TOL 1.e-7
#define EPS 1.e-10

static PJ_XY omerc_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_omerc_data *Q = static_cast<struct pj_omerc_data *>(P->opaque);
    double u, v;

    if (fabs(fabs(lp.phi) - M_HALFPI) > EPS) {
        const double W = Q->E / pow(pj_tsfn(lp.phi, sin(lp.phi), P->e), Q->B);
        const double one_div_W = 1. / W;
        const double S = .5 * (W - one_div_W);
        const double T = .5 * (W + one_div_W);
        const double V = sin(Q->B * lp.lam);
        const double U = (S * Q->singam - V * Q->cosgam) / T;
        if (fabs(fabs(U) - 1.0) < EPS) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        v = 0.5 * Q->ArB * log((1. - U) / (1. + U));
        const double temp = cos(Q->B * lp.lam);
        if (fabs(temp) < TOL) {
            u = Q->A * lp.lam;
        } else {
            u = Q->ArB * atan2((S * Q->cosgam + V * Q->singam), temp);
        }
    } else {
        v = lp.phi > 0 ? Q->v_pole_n : Q->v_pole_s;
        u = Q->ArB * lp.phi;
    }
    if (Q->no_rot) {
        xy.x = u;
        xy.y = v;
    } else {
        u -= Q->u_0;
        xy.x = v * Q->cosrot + u * Q->sinrot;
        xy.y = u * Q->cosrot - v * Q->sinrot;
    }
    return xy;
}

static PJ_LP omerc_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_omerc_data *Q = static_cast<struct pj_omerc_data *>(P->opaque);
    double u, v, Qp, Sp, Tp, Vp, Up;

    if (Q->no_rot) {
        v = xy.y;
        u = xy.x;
    } else {
        v = xy.x * Q->cosrot - xy.y * Q->sinrot;
        u = xy.y * Q->cosrot + xy.x * Q->sinrot + Q->u_0;
    }
    Qp = exp(-Q->BrA * v);
    if (Qp == 0) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().lp;
    }
    Sp = .5 * (Qp - 1. / Qp);
    Tp = .5 * (Qp + 1. / Qp);
    Vp = sin(Q->BrA * u);
    Up = (Vp * Q->cosgam + Sp * Q->singam) / Tp;
    if (fabs(fabs(Up) - 1.) < EPS) {
        lp.lam = 0.;
        lp.phi = Up < 0. ? -M_HALFPI : M_HALFPI;
    } else {
        lp.phi = Q->E / sqrt((1. + Up) / (1. - Up));
        if ((lp.phi = pj_phi2(P->ctx, pow(lp.phi, 1. / Q->B), P->e)) ==
            HUGE_VAL) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        }
        lp.lam =
            -Q->rB * atan2((Sp * Q->cosgam - Vp * Q->singam), cos(Q->BrA * u));
    }
    return lp;
}

PJ *PJ_PROJECTION(omerc) {
    double con, com, cosph0, D, F, H, L, sinph0, p, J,
        gamma = 0, gamma0, lamc = 0, lam1 = 0, lam2 = 0, phi1 = 0, phi2 = 0,
        alpha_c = 0;
    int alp, gam, no_off = 0;

    struct pj_omerc_data *Q = static_cast<struct pj_omerc_data *>(
        calloc(1, sizeof(struct pj_omerc_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->no_rot = pj_param(P->ctx, P->params, "bno_rot").i;
    if ((alp = pj_param(P->ctx, P->params, "talpha").i) != 0)
        alpha_c = pj_param(P->ctx, P->params, "ralpha").f;
    if ((gam = pj_param(P->ctx, P->params, "tgamma").i) != 0)
        gamma = pj_param(P->ctx, P->params, "rgamma").f;
    if (alp || gam) {
        lamc = pj_param(P->ctx, P->params, "rlonc").f;
        no_off =
            /* For libproj4 compatibility */
            pj_param(P->ctx, P->params, "tno_off").i
            /* for backward compatibility */
            || pj_param(P->ctx, P->params, "tno_uoff").i;
        if (no_off) {
            /* Mark the parameter as used, so that the pj_get_def() return them
             */
            pj_param(P->ctx, P->params, "sno_uoff");
            pj_param(P->ctx, P->params, "sno_off");
        }
    } else {
        lam1 = pj_param(P->ctx, P->params, "rlon_1").f;
        phi1 = pj_param(P->ctx, P->params, "rlat_1").f;
        lam2 = pj_param(P->ctx, P->params, "rlon_2").f;
        phi2 = pj_param(P->ctx, P->params, "rlat_2").f;
        con = fabs(phi1);

        if (fabs(phi1) > M_HALFPI - TOL) {
            proj_log_error(
                P, _("Invalid value for lat_1: |lat_1| should be < 90°"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        if (fabs(phi2) > M_HALFPI - TOL) {
            proj_log_error(
                P, _("Invalid value for lat_2: |lat_2| should be < 90°"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        if (fabs(phi1 - phi2) <= TOL) {
            proj_log_error(P,
                           _("Invalid value for lat_1/lat_2: lat_1 should be "
                             "different from lat_2"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        if (con <= TOL) {
            proj_log_error(
                P,
                _("Invalid value for lat_1: lat_1 should be different from 0"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        if (fabs(fabs(P->phi0) - M_HALFPI) <= TOL) {
            proj_log_error(
                P, _("Invalid value for lat_0: |lat_0| should be < 90°"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    if (pj_param(P->ctx, P->params, "rlon_0").i) {
        proj_log_trace(P, _("lon_0 is ignored."));
    }

    com = sqrt(P->one_es);
    if (fabs(P->phi0) > EPS) {
        sinph0 = sin(P->phi0);
        cosph0 = cos(P->phi0);
        con = 1. - P->es * sinph0 * sinph0;
        Q->B = cosph0 * cosph0;
        Q->B = sqrt(1. + P->es * Q->B * Q->B / P->one_es);
        Q->A = Q->B * P->k0 * com / con;
        D = Q->B * com / (cosph0 * sqrt(con));
        if ((F = D * D - 1.) <= 0.)
            F = 0.;
        else {
            F = sqrt(F);
            if (P->phi0 < 0.)
                F = -F;
        }
        Q->E = F += D;
        Q->E *= pow(pj_tsfn(P->phi0, sinph0, P->e), Q->B);
    } else {
        Q->B = 1. / com;
        Q->A = P->k0;
        Q->E = D = F = 1.;
    }
    if (alp || gam) {
        if (alp) {
            gamma0 = aasin(P->ctx, sin(alpha_c) / D);
            if (!gam)
                gamma = alpha_c;
        } else {
            gamma0 = gamma;
            alpha_c = aasin(P->ctx, D * sin(gamma0));
            if (proj_errno(P) != 0) {
                // For a sphere, |gamma| must be <= 90 - |lat_0|
                // On an ellipsoid, this is very slightly above
                proj_log_error(P,
                               ("Invalid value for gamma: given lat_0 value, "
                                "|gamma| should be <= " +
                                std::to_string(asin(1. / D) / M_PI * 180) + "°")
                                   .c_str());
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
        }

        if (fabs(fabs(P->phi0) - M_HALFPI) <= TOL) {
            proj_log_error(
                P, _("Invalid value for lat_0: |lat_0| should be < 90°"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        P->lam0 = lamc - aasin(P->ctx, .5 * (F - 1. / F) * tan(gamma0)) / Q->B;
    } else {
        H = pow(pj_tsfn(phi1, sin(phi1), P->e), Q->B);
        L = pow(pj_tsfn(phi2, sin(phi2), P->e), Q->B);
        F = Q->E / H;
        p = (L - H) / (L + H);
        if (p == 0) {
            // Not quite, but es is very close to 1...
            proj_log_error(P, _("Invalid value for eccentricity"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        J = Q->E * Q->E;
        J = (J - L * H) / (J + L * H);
        if ((con = lam1 - lam2) < -M_PI)
            lam2 -= M_TWOPI;
        else if (con > M_PI)
            lam2 += M_TWOPI;
        P->lam0 = adjlon(.5 * (lam1 + lam2) -
                         atan(J * tan(.5 * Q->B * (lam1 - lam2)) / p) / Q->B);
        const double denom = F - 1. / F;
        if (denom == 0) {
            proj_log_error(P, _("Invalid value for eccentricity"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        gamma0 = atan(2. * sin(Q->B * adjlon(lam1 - P->lam0)) / denom);
        gamma = alpha_c = aasin(P->ctx, D * sin(gamma0));
    }
    Q->singam = sin(gamma0);
    Q->cosgam = cos(gamma0);
    Q->sinrot = sin(gamma);
    Q->cosrot = cos(gamma);
    Q->BrA = 1. / (Q->ArB = Q->A * (Q->rB = 1. / Q->B));
    Q->AB = Q->A * Q->B;
    if (no_off)
        Q->u_0 = 0;
    else {
        Q->u_0 = fabs(Q->ArB * atan(sqrt(D * D - 1.) / cos(alpha_c)));
        if (P->phi0 < 0.)
            Q->u_0 = -Q->u_0;
    }
    F = 0.5 * gamma0;
    Q->v_pole_n = Q->ArB * log(tan(M_FORTPI - F));
    Q->v_pole_s = Q->ArB * log(tan(M_FORTPI + F));
    P->inv = omerc_e_inverse;
    P->fwd = omerc_e_forward;

    return P;
}

#undef TOL
#undef EPS
