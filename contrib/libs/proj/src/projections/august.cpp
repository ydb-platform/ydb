

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(august, "August Epicycloidal") "\n\tMisc Sph, no inv";
#define M 1.333333333333333

static PJ_XY august_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double t, c1, c, x1, x12, y1, y12;
    (void)P;

    t = tan(.5 * lp.phi);
    c1 = sqrt(1. - t * t);
    lp.lam *= .5;
    c = 1. + c1 * cos(lp.lam);
    x1 = sin(lp.lam) * c1 / c;
    y1 = t / c;
    x12 = x1 * x1;
    y12 = y1 * y1;
    xy.x = M * x1 * (3. + x12 - 3. * y12);
    xy.y = M * y1 * (3. + 3. * x12 - y12);
    return (xy);
}

PJ *PJ_PROJECTION(august) {
    P->inv = nullptr;
    P->fwd = august_s_forward;
    P->es = 0.;
    return P;
}
