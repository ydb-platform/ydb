

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(mbt_fps, "McBryde-Thomas Flat-Pole Sine (No. 2)") "\n\tCyl, Sph";

#define MAX_ITER 10
#define LOOP_TOL 1e-7
#define C1 0.45503
#define C2 1.36509
#define C3 1.41546
#define C_x 0.22248
#define C_y 1.44492
#define C1_2 0.33333333333333333333333333

static PJ_XY mbt_fps_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    const double k = C3 * sin(lp.phi);
    for (int i = MAX_ITER; i; --i) {
        const double t = lp.phi / C2;
        const double V =
            (C1 * sin(t) + sin(lp.phi) - k) / (C1_2 * cos(t) + cos(lp.phi));
        lp.phi -= V;
        if (fabs(V) < LOOP_TOL)
            break;
    }
    const double t = lp.phi / C2;
    xy.x = C_x * lp.lam * (1. + 3. * cos(lp.phi) / cos(t));
    xy.y = C_y * sin(t);
    return xy;
}

static PJ_LP mbt_fps_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};

    const double t = aasin(P->ctx, xy.y / C_y);
    lp.phi = C2 * t;
    lp.lam = xy.x / (C_x * (1. + 3. * cos(lp.phi) / cos(t)));
    lp.phi = aasin(P->ctx, (C1 * sin(t) + sin(lp.phi)) / C3);
    return (lp);
}

PJ *PJ_PROJECTION(mbt_fps) {

    P->es = 0;
    P->inv = mbt_fps_s_inverse;
    P->fwd = mbt_fps_s_forward;

    return P;
}

#undef MAX_ITER
#undef LOOP_TOL
#undef C1
#undef C2
#undef C3
#undef C_x
#undef C_y
#undef C1_2
