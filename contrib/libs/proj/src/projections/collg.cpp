

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(collg, "Collignon") "\n\tPCyl, Sph";
#define FXC 1.12837916709551257390
#define FYC 1.77245385090551602729
#define ONEEPS 1.0000001

static PJ_XY collg_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;
    xy.y = 1. - sin(lp.phi);
    if (xy.y <= 0.)
        xy.y = 0.;
    else
        xy.y = sqrt(xy.y);
    xy.x = FXC * lp.lam * xy.y;
    xy.y = FYC * (1. - xy.y);
    return (xy);
}

static PJ_LP collg_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    lp.phi = xy.y / FYC - 1.;
    lp.phi = 1. - lp.phi * lp.phi;
    if (fabs(lp.phi) < 1.)
        lp.phi = asin(lp.phi);
    else if (fabs(lp.phi) > ONEEPS) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    } else {
        lp.phi = lp.phi < 0. ? -M_HALFPI : M_HALFPI;
    }

    lp.lam = 1. - sin(lp.phi);
    if (lp.lam <= 0.)
        lp.lam = 0.;
    else
        lp.lam = xy.x / (FXC * sqrt(lp.lam));
    return (lp);
}

PJ *PJ_PROJECTION(collg) {
    P->es = 0.0;
    P->inv = collg_s_inverse;
    P->fwd = collg_s_forward;

    return P;
}

#undef FXC
#undef FYC
#undef ONEEPS
