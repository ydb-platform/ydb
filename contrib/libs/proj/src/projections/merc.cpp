

#include <float.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"
#include <math.h>

PROJ_HEAD(merc, "Mercator") "\n\tCyl, Sph&Ell\n\tlat_ts=";
PROJ_HEAD(webmerc, "Web Mercator / Pseudo Mercator") "\n\tCyl, Ell\n\t";

static PJ_XY merc_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    xy.x = P->k0 * lp.lam;
    // Instead of calling tan and sin, call sin and cos which the compiler
    // optimizes to a single call to sincos.
    double sphi = sin(lp.phi);
    double cphi = cos(lp.phi);
    xy.y = P->k0 * (asinh(sphi / cphi) - P->e * atanh(P->e * sphi));
    return xy;
}

static PJ_XY merc_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    xy.x = P->k0 * lp.lam;
    xy.y = P->k0 * asinh(tan(lp.phi));
    return xy;
}

static PJ_LP merc_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    lp.phi = atan(pj_sinhpsi2tanphi(P->ctx, sinh(xy.y / P->k0), P->e));
    lp.lam = xy.x / P->k0;
    return lp;
}

static PJ_LP merc_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    lp.phi = atan(sinh(xy.y / P->k0));
    lp.lam = xy.x / P->k0;
    return lp;
}

PJ *PJ_PROJECTION(merc) {
    double phits = 0.0;
    int is_phits;

    if ((is_phits = pj_param(P->ctx, P->params, "tlat_ts").i)) {
        phits = fabs(pj_param(P->ctx, P->params, "rlat_ts").f);
        if (phits >= M_HALFPI) {
            proj_log_error(
                P, _("Invalid value for lat_ts: |lat_ts| should be <= 90°"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    if (P->es != 0.0) { /* ellipsoid */
        if (is_phits)
            P->k0 = pj_msfn(sin(phits), cos(phits), P->es);
        P->inv = merc_e_inverse;
        P->fwd = merc_e_forward;
    }

    else { /* sphere */
        if (is_phits)
            P->k0 = cos(phits);
        P->inv = merc_s_inverse;
        P->fwd = merc_s_forward;
    }

    return P;
}

PJ *PJ_PROJECTION(webmerc) {

    /* Overriding k_0 with fixed parameter */
    P->k0 = 1.0;

    P->inv = merc_s_inverse;
    P->fwd = merc_s_forward;
    return P;
}
