

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(tcc, "Transverse Central Cylindrical") "\n\tCyl, Sph, no inv";

#define EPS10 1.e-10

static PJ_XY tcc_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};

    const double b = cos(lp.phi) * sin(lp.lam);
    const double bt = 1. - b * b;
    if (bt < EPS10) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    xy.x = b / sqrt(bt);
    xy.y = atan2(tan(lp.phi), cos(lp.lam));
    return xy;
}

PJ *PJ_PROJECTION(tcc) {
    P->es = 0.;
    P->fwd = tcc_s_forward;
    P->inv = nullptr;

    return P;
}
