

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(wag2, "Wagner II") "\n\tPCyl, Sph";

#define C_x 0.92483
#define C_y 1.38725
#define C_p1 0.88022
#define C_p2 0.88550

static PJ_XY wag2_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    lp.phi = aasin(P->ctx, C_p1 * sin(C_p2 * lp.phi));
    xy.x = C_x * lp.lam * cos(lp.phi);
    xy.y = C_y * lp.phi;
    return (xy);
}

static PJ_LP wag2_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    lp.phi = xy.y / C_y;
    lp.lam = xy.x / (C_x * cos(lp.phi));
    lp.phi = aasin(P->ctx, sin(lp.phi) / C_p1) / C_p2;
    return (lp);
}

PJ *PJ_PROJECTION(wag2) {
    P->es = 0.;
    P->inv = wag2_s_inverse;
    P->fwd = wag2_s_forward;
    return P;
}

#undef C_x
#undef C_y
#undef C_p1
#undef C_p2
