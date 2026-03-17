

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(gall, "Gall (Gall Stereographic)") "\n\tCyl, Sph";

#define YF 1.70710678118654752440
#define XF 0.70710678118654752440
#define RYF 0.58578643762690495119
#define RXF 1.41421356237309504880

static PJ_XY gall_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    xy.x = XF * lp.lam;
    xy.y = YF * tan(.5 * lp.phi);

    return xy;
}

static PJ_LP gall_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    (void)P;

    lp.lam = RXF * xy.x;
    lp.phi = 2. * atan(xy.y * RYF);

    return lp;
}

PJ *PJ_PROJECTION(gall) {
    P->es = 0.0;

    P->inv = gall_s_inverse;
    P->fwd = gall_s_forward;

    return P;
}
