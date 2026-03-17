

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(nell, "Nell") "\n\tPCyl, Sph";

#define MAX_ITER 10
#define LOOP_TOL 1e-7

static PJ_XY nell_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    int i;
    (void)P;

    const double k = 2. * sin(lp.phi);
    const double phi_pow_2 = lp.phi * lp.phi;
    lp.phi *= 1.00371 + phi_pow_2 * (-0.0935382 + phi_pow_2 * -0.011412);
    for (i = MAX_ITER; i; --i) {
        const double V = (lp.phi + sin(lp.phi) - k) / (1. + cos(lp.phi));
        lp.phi -= V;
        if (fabs(V) < LOOP_TOL)
            break;
    }
    xy.x = 0.5 * lp.lam * (1. + cos(lp.phi));
    xy.y = lp.phi;

    return xy;
}

static PJ_LP nell_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    lp.lam = 2. * xy.x / (1. + cos(xy.y));
    lp.phi = aasin(P->ctx, 0.5 * (xy.y + sin(xy.y)));

    return lp;
}

PJ *PJ_PROJECTION(nell) {

    P->es = 0;
    P->inv = nell_s_inverse;
    P->fwd = nell_s_forward;

    return P;
}

#undef MAX_ITER
#undef LOOP_TOL
