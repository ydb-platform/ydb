

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(mill, "Miller Cylindrical") "\n\tCyl, Sph";

static PJ_XY mill_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    xy.x = lp.lam;
    xy.y = log(tan(M_FORTPI + lp.phi * .4)) * 1.25;

    return (xy);
}

static PJ_LP mill_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    (void)P;

    lp.lam = xy.x;
    lp.phi = 2.5 * (atan(exp(.8 * xy.y)) - M_FORTPI);

    return (lp);
}

PJ *PJ_PROJECTION(mill) {
    P->es = 0.;
    P->inv = mill_s_inverse;
    P->fwd = mill_s_forward;

    return P;
}
