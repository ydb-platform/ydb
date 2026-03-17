

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(lask, "Laskowski") "\n\tMisc Sph, no inv";

#define a10 0.975534
#define a12 -0.119161
#define a32 -0.0143059
#define a14 -0.0547009
#define b01 1.00384
#define b21 0.0802894
#define b03 0.0998909
#define b41 0.000199025
#define b23 -0.0285500
#define b05 -0.0491032

static PJ_XY lask_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double l2, p2;
    (void)P;

    l2 = lp.lam * lp.lam;
    p2 = lp.phi * lp.phi;
    xy.x = lp.lam * (a10 + p2 * (a12 + l2 * a32 + p2 * a14));
    xy.y = lp.phi *
           (b01 + l2 * (b21 + p2 * b23 + l2 * b41) + p2 * (b03 + p2 * b05));
    return xy;
}

PJ *PJ_PROJECTION(lask) {

    P->fwd = lask_s_forward;
    P->es = 0.;

    return P;
}
