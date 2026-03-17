
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(eck1, "Eckert I") "\n\tPCyl, Sph";
#define FC 0.92131773192356127802
#define RP 0.31830988618379067154

static PJ_XY eck1_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    xy.x = FC * lp.lam * (1. - RP * fabs(lp.phi));
    xy.y = FC * lp.phi;

    return xy;
}

static PJ_LP eck1_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    (void)P;

    lp.phi = xy.y / FC;
    lp.lam = xy.x / (FC * (1. - RP * fabs(lp.phi)));

    return (lp);
}

PJ *PJ_PROJECTION(eck1) {
    P->es = 0.0;
    P->inv = eck1_s_inverse;
    P->fwd = eck1_s_forward;

    return P;
}
