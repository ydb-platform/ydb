
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(denoy, "Denoyer Semi-Elliptical") "\n\tPCyl, no inv, Sph";

#define C0 0.95
#define C1 -0.08333333333333333333
#define C3 0.00166666666666666666
#define D1 0.9
#define D5 0.03

static PJ_XY denoy_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;
    xy.y = lp.phi;
    xy.x = lp.lam;
    lp.lam = fabs(lp.lam);
    xy.x *= cos((C0 + lp.lam * (C1 + lp.lam * lp.lam * C3)) *
                (lp.phi * (D1 + D5 * lp.phi * lp.phi * lp.phi * lp.phi)));
    return xy;
}

PJ *PJ_PROJECTION(denoy) {
    P->es = 0.0;
    P->fwd = denoy_s_forward;

    return P;
}

#undef C0
#undef C1
#undef C3
#undef D1
#undef D5
