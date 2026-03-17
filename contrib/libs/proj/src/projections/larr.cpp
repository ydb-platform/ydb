

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(larr, "Larrivee") "\n\tMisc Sph, no inv";

#define SIXTH .16666666666666666

static PJ_XY larr_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    xy.x = 0.5 * lp.lam * (1. + sqrt(cos(lp.phi)));
    xy.y = lp.phi / (cos(0.5 * lp.phi) * cos(SIXTH * lp.lam));
    return xy;
}

PJ *PJ_PROJECTION(larr) {

    P->es = 0;
    P->fwd = larr_s_forward;

    return P;
}
