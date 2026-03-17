
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(boggs, "Boggs Eumorphic") "\n\tPCyl, no inv, Sph";
#define NITER 20
#define EPS 1e-7
#define FXC 2.00276
#define FXC2 1.11072
#define FYC 0.49931

static PJ_XY boggs_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double theta, th1, c;
    int i;
    (void)P;

    theta = lp.phi;
    if (fabs(fabs(lp.phi) - M_HALFPI) < EPS)
        xy.x = 0.;
    else {
        c = sin(theta) * M_PI;
        for (i = NITER; i; --i) {
            th1 = (theta + sin(theta) - c) / (1. + cos(theta));
            theta -= th1;
            if (fabs(th1) < EPS)
                break;
        }
        theta *= 0.5;
        xy.x = FXC * lp.lam / (1. / cos(lp.phi) + FXC2 / cos(theta));
    }
    xy.y = FYC * (lp.phi + M_SQRT2 * sin(theta));
    return (xy);
}

PJ *PJ_PROJECTION(boggs) {
    P->es = 0.;
    P->fwd = boggs_s_forward;
    return P;
}

#undef NITER
#undef EPS
#undef FXC
#undef FXC2
#undef FYC
