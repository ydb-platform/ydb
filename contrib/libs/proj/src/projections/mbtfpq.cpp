

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(mbtfpq, "McBryde-Thomas Flat-Polar Quartic") "\n\tCyl, Sph";

#define NITER 20
#define EPS 1e-7
#define ONETOL 1.000001
#define C 1.70710678118654752440
#define RC 0.58578643762690495119
#define FYC 1.87475828462269495505
#define RYC 0.53340209679417701685
#define FXC 0.31245971410378249250
#define RXC 3.20041258076506210122

static PJ_XY mbtfpq_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    const double c = C * sin(lp.phi);
    for (int i = NITER; i; --i) {
        const double th1 = (sin(.5 * lp.phi) + sin(lp.phi) - c) /
                           (.5 * cos(.5 * lp.phi) + cos(lp.phi));
        lp.phi -= th1;
        if (fabs(th1) < EPS)
            break;
    }
    xy.x = FXC * lp.lam * (1.0 + 2. * cos(lp.phi) / cos(0.5 * lp.phi));
    xy.y = FYC * sin(0.5 * lp.phi);
    return xy;
}

static PJ_LP mbtfpq_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double t;

    lp.phi = RYC * xy.y;
    if (fabs(lp.phi) > 1.) {
        if (fabs(lp.phi) > ONETOL) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else if (lp.phi < 0.) {
            t = -1.;
            lp.phi = -M_PI;
        } else {
            t = 1.;
            lp.phi = M_PI;
        }
    } else {
        t = lp.phi;
        lp.phi = 2. * asin(lp.phi);
    }
    lp.lam = RXC * xy.x / (1. + 2. * cos(lp.phi) / cos(0.5 * lp.phi));
    lp.phi = RC * (t + sin(lp.phi));
    if (fabs(lp.phi) > 1.)
        if (fabs(lp.phi) > ONETOL) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else
            lp.phi = lp.phi < 0. ? -M_HALFPI : M_HALFPI;
    else
        lp.phi = asin(lp.phi);
    return lp;
}

PJ *PJ_PROJECTION(mbtfpq) {

    P->es = 0.;
    P->inv = mbtfpq_s_inverse;
    P->fwd = mbtfpq_s_forward;

    return P;
}

#undef NITER
#undef EPS
#undef ONETOL
#undef C
#undef RC
#undef FYC
#undef RYC
#undef FXC
#undef RXC
