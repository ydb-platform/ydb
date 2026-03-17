

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(nell_h, "Nell-Hammer") "\n\tPCyl, Sph";

#define NITER 9
#define EPS 1e-7

static PJ_XY nell_h_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    xy.x = 0.5 * lp.lam * (1. + cos(lp.phi));
    xy.y = 2.0 * (lp.phi - tan(0.5 * lp.phi));

    return xy;
}

static PJ_LP nell_h_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    int i;
    (void)P;

    const double p = 0.5 * xy.y;
    for (i = NITER; i; --i) {
        const double c = cos(0.5 * lp.phi);
        const double V = (lp.phi - tan(lp.phi / 2) - p) / (1. - 0.5 / (c * c));
        lp.phi -= V;
        if (fabs(V) < EPS)
            break;
    }
    if (!i) {
        lp.phi = p < 0. ? -M_HALFPI : M_HALFPI;
        lp.lam = 2. * xy.x;
    } else
        lp.lam = 2. * xy.x / (1. + cos(lp.phi));

    return lp;
}

PJ *PJ_PROJECTION(nell_h) {
    P->es = 0.;
    P->inv = nell_h_s_inverse;
    P->fwd = nell_h_s_forward;

    return P;
}

#undef NITER
#undef EPS
