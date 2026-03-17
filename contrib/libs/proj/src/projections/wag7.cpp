

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(wag7, "Wagner VII") "\n\tMisc Sph, no inv";

static PJ_XY wag7_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};

    (void)P; /* Shut up compiler warnings about unused P */

    xy.y = 0.90630778703664996 * sin(lp.phi);
    const double theta = asin(xy.y);
    const double ct = cos(theta);
    lp.lam /= 3.;
    xy.x = 2.66723 * ct * sin(lp.lam);
    const double D = 1 / (sqrt(0.5 * (1 + ct * cos(lp.lam))));
    xy.y *= 1.24104 * D;
    xy.x *= D;
    return (xy);
}

PJ *PJ_PROJECTION(wag7) {
    P->fwd = wag7_s_forward;
    P->inv = nullptr;
    P->es = 0.;
    return P;
}
