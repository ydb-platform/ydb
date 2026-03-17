

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(cc, "Central Cylindrical") "\n\tCyl, Sph";
#define EPS10 1.e-10

static PJ_XY cc_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    if (fabs(fabs(lp.phi) - M_HALFPI) <= EPS10) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    xy.x = lp.lam;
    xy.y = tan(lp.phi);
    return xy;
}

static PJ_LP cc_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    (void)P;
    lp.phi = atan(xy.y);
    lp.lam = xy.x;
    return lp;
}

PJ *PJ_PROJECTION(cc) {
    P->es = 0.;

    P->inv = cc_s_inverse;
    P->fwd = cc_s_forward;

    return P;
}
