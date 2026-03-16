

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(mbtfpp, "McBride-Thomas Flat-Polar Parabolic") "\n\tCyl, Sph";

#define CSy .95257934441568037152
#define FXC .92582009977255146156
#define FYC 3.40168025708304504493
#define C23 .66666666666666666666
#define C13 .33333333333333333333
#define ONEEPS 1.0000001

static PJ_XY mbtfpp_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    lp.phi = asin(CSy * sin(lp.phi));
    xy.x = FXC * lp.lam * (2. * cos(C23 * lp.phi) - 1.);
    xy.y = FYC * sin(C13 * lp.phi);
    return xy;
}

static PJ_LP mbtfpp_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};

    lp.phi = xy.y / FYC;
    if (fabs(lp.phi) >= 1.) {
        if (fabs(lp.phi) > ONEEPS) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else {
            lp.phi = (lp.phi < 0.) ? -M_HALFPI : M_HALFPI;
        }
    } else
        lp.phi = asin(lp.phi);

    lp.phi *= 3.;
    lp.lam = xy.x / (FXC * (2. * cos(C23 * lp.phi) - 1.));
    lp.phi = sin(lp.phi) / CSy;
    if (fabs(lp.phi) >= 1.) {
        if (fabs(lp.phi) > ONEEPS) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else {
            lp.phi = (lp.phi < 0.) ? -M_HALFPI : M_HALFPI;
        }
    } else
        lp.phi = asin(lp.phi);

    return lp;
}

PJ *PJ_PROJECTION(mbtfpp) {

    P->es = 0.;
    P->inv = mbtfpp_s_inverse;
    P->fwd = mbtfpp_s_forward;

    return P;
}

#undef CSy
#undef FXC
#undef FYC
#undef C23
#undef C13
#undef ONEEPS
