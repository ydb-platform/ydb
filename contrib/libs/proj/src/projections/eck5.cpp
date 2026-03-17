

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(eck5, "Eckert V") "\n\tPCyl, Sph";

#define XF 0.44101277172455148219
#define RXF 2.26750802723822639137
#define YF 0.88202554344910296438
#define RYF 1.13375401361911319568

static PJ_XY eck5_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;
    xy.x = XF * (1. + cos(lp.phi)) * lp.lam;
    xy.y = YF * lp.phi;

    return xy;
}

static PJ_LP eck5_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    (void)P;
    lp.phi = RYF * xy.y;
    lp.lam = RXF * xy.x / (1. + cos(lp.phi));

    return lp;
}

PJ *PJ_PROJECTION(eck5) {
    P->es = 0.0;
    P->inv = eck5_s_inverse;
    P->fwd = eck5_s_forward;

    return P;
}
