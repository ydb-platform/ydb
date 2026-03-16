
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(lcc, "Lambert Conformal Conic")
"\n\tConic, Sph&Ell\n\tlat_1= and lat_2= or lat_0, k_0=";

#define EPS10 1.e-10

namespace { // anonymous namespace
struct pj_lcc_data {
    double phi1;
    double phi2;
    double n;
    double rho0;
    double c;
};
} // anonymous namespace

static PJ_XY lcc_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0., 0.};
    struct pj_lcc_data *Q = static_cast<struct pj_lcc_data *>(P->opaque);
    double rho;

    if (fabs(fabs(lp.phi) - M_HALFPI) < EPS10) {
        if ((lp.phi * Q->n) <= 0.) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        rho = 0.;
    } else {
        rho =
            Q->c * (P->es != 0. ? pow(pj_tsfn(lp.phi, sin(lp.phi), P->e), Q->n)
                                : pow(tan(M_FORTPI + .5 * lp.phi), -Q->n));
    }
    lp.lam *= Q->n;
    xy.x = P->k0 * (rho * sin(lp.lam));
    xy.y = P->k0 * (Q->rho0 - rho * cos(lp.lam));
    return xy;
}

static PJ_LP lcc_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0., 0.};
    struct pj_lcc_data *Q = static_cast<struct pj_lcc_data *>(P->opaque);
    double rho;

    xy.x /= P->k0;
    xy.y /= P->k0;

    xy.y = Q->rho0 - xy.y;
    rho = hypot(xy.x, xy.y);
    if (rho != 0.) {
        if (Q->n < 0.) {
            rho = -rho;
            xy.x = -xy.x;
            xy.y = -xy.y;
        }
        if (P->es != 0.) {
            lp.phi = pj_phi2(P->ctx, pow(rho / Q->c, 1. / Q->n), P->e);
            if (lp.phi == HUGE_VAL) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return lp;
            }

        } else
            lp.phi = 2. * atan(pow(Q->c / rho, 1. / Q->n)) - M_HALFPI;
        lp.lam = atan2(xy.x, xy.y) / Q->n;
    } else {
        lp.lam = 0.;
        lp.phi = Q->n > 0. ? M_HALFPI : -M_HALFPI;
    }
    return lp;
}

PJ *PJ_PROJECTION(lcc) {
    double cosphi, sinphi;
    int secant;
    struct pj_lcc_data *Q = static_cast<struct pj_lcc_data *>(
        calloc(1, sizeof(struct pj_lcc_data)));

    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->phi1 = pj_param(P->ctx, P->params, "rlat_1").f;
    if (pj_param(P->ctx, P->params, "tlat_2").i)
        Q->phi2 = pj_param(P->ctx, P->params, "rlat_2").f;
    else {
        Q->phi2 = Q->phi1;
        if (!pj_param(P->ctx, P->params, "tlat_0").i)
            P->phi0 = Q->phi1;
    }

    if (fabs(Q->phi1 + Q->phi2) < EPS10) {
        proj_log_error(P, _("Invalid value for lat_1 and lat_2: |lat_1 + "
                            "lat_2| should be > 0"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    Q->n = sinphi = sin(Q->phi1);
    cosphi = cos(Q->phi1);

    if (fabs(cosphi) < EPS10 || fabs(Q->phi1) >= M_PI_2) {
        proj_log_error(P,
                       _("Invalid value for lat_1: |lat_1| should be < 90°"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (fabs(cos(Q->phi2)) < EPS10 || fabs(Q->phi2) >= M_PI_2) {
        proj_log_error(P,
                       _("Invalid value for lat_2: |lat_2| should be < 90°"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    secant = fabs(Q->phi1 - Q->phi2) >= EPS10;
    if (P->es != 0.) {
        double ml1, m1;

        m1 = pj_msfn(sinphi, cosphi, P->es);
        ml1 = pj_tsfn(Q->phi1, sinphi, P->e);
        if (secant) { /* secant cone */
            sinphi = sin(Q->phi2);
            Q->n = log(m1 / pj_msfn(sinphi, cos(Q->phi2), P->es));
            if (Q->n == 0) {
                // Not quite, but es is very close to 1...
                proj_log_error(P, _("Invalid value for eccentricity"));
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
            const double ml2 = pj_tsfn(Q->phi2, sinphi, P->e);
            const double denom = log(ml1 / ml2);
            if (denom == 0) {
                // Not quite, but es is very close to 1...
                proj_log_error(P, _("Invalid value for eccentricity"));
                return pj_default_destructor(
                    P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
            Q->n /= denom;
        }
        Q->rho0 = m1 * pow(ml1, -Q->n) / Q->n;
        Q->c = Q->rho0;
        Q->rho0 *= (fabs(fabs(P->phi0) - M_HALFPI) < EPS10)
                       ? 0.
                       : pow(pj_tsfn(P->phi0, sin(P->phi0), P->e), Q->n);
    } else {
        if (secant)
            Q->n =
                log(cosphi / cos(Q->phi2)) / log(tan(M_FORTPI + .5 * Q->phi2) /
                                                 tan(M_FORTPI + .5 * Q->phi1));
        if (Q->n == 0) {
            // Likely reason is that phi1 / phi2 are too close to zero.
            // Can be reproduced with +proj=lcc +a=1 +lat_2=.0000001
            proj_log_error(
                P, _("Invalid value for lat_1 and lat_2: |lat_1 + lat_2| "
                     "should be > 0"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        Q->c = cosphi * pow(tan(M_FORTPI + .5 * Q->phi1), Q->n) / Q->n;
        Q->rho0 = (fabs(fabs(P->phi0) - M_HALFPI) < EPS10)
                      ? 0.
                      : Q->c * pow(tan(M_FORTPI + .5 * P->phi0), -Q->n);
    }

    P->inv = lcc_e_inverse;
    P->fwd = lcc_e_forward;

    return P;
}

#undef EPS10
