

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(gins8, "Ginsburg VIII (TsNIIGAiK)") "\n\tPCyl, Sph, no inv";

#define Cl 0.000952426
#define Cp 0.162388
#define C12 0.08333333333333333

static PJ_XY gins8_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double t = lp.phi * lp.phi;
    (void)P;

    xy.y = lp.phi * (1. + t * C12);
    xy.x = lp.lam * (1. - Cp * t);
    t = lp.lam * lp.lam;
    xy.x *= (0.87 - Cl * t * t);

    return xy;
}

PJ *PJ_PROJECTION(gins8) {
    P->es = 0.0;
    P->inv = nullptr;
    P->fwd = gins8_s_forward;

    return P;
}
