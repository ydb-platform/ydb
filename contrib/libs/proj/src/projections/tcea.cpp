

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(tcea, "Transverse Cylindrical Equal Area") "\n\tCyl, Sph";

static PJ_XY tcea_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    xy.x = cos(lp.phi) * sin(lp.lam) / P->k0;
    xy.y = P->k0 * (atan2(tan(lp.phi), cos(lp.lam)) - P->phi0);
    return xy;
}

static PJ_LP tcea_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double t;

    xy.y = xy.y / P->k0 + P->phi0;
    xy.x *= P->k0;
    t = sqrt(1. - xy.x * xy.x);
    lp.phi = asin(t * sin(xy.y));
    lp.lam = atan2(xy.x, t * cos(xy.y));
    return lp;
}

PJ *PJ_PROJECTION(tcea) {
    P->inv = tcea_s_inverse;
    P->fwd = tcea_s_forward;
    P->es = 0.;
    return P;
}
