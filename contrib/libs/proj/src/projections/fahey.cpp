

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(fahey, "Fahey") "\n\tPcyl, Sph";

#define TOL 1e-6

static PJ_XY fahey_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    xy.x = tan(0.5 * lp.phi);
    xy.y = 1.819152 * xy.x;
    xy.x = 0.819152 * lp.lam * asqrt(1 - xy.x * xy.x);
    return xy;
}

static PJ_LP fahey_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    (void)P;

    xy.y /= 1.819152;
    lp.phi = 2. * atan(xy.y);
    xy.y = 1. - xy.y * xy.y;
    lp.lam = fabs(xy.y) < TOL ? 0. : xy.x / (0.819152 * sqrt(xy.y));
    return lp;
}

PJ *PJ_PROJECTION(fahey) {
    P->es = 0.;
    P->inv = fahey_s_inverse;
    P->fwd = fahey_s_forward;

    return P;
}
