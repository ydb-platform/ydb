

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(wink2, "Winkel II") "\n\tPCyl, Sph\n\tlat_1=";

namespace { // anonymous namespace
struct pj_wink2_data {
    double cosphi1;
};
} // anonymous namespace

#define MAX_ITER 10
#define LOOP_TOL 1e-7

static PJ_XY wink2_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    int i;

    xy.y = lp.phi * M_TWO_D_PI;
    const double k = M_PI * sin(lp.phi);
    lp.phi *= 1.8;
    for (i = MAX_ITER; i; --i) {
        const double V = (lp.phi + sin(lp.phi) - k) / (1. + cos(lp.phi));
        lp.phi -= V;
        if (fabs(V) < LOOP_TOL)
            break;
    }
    if (!i)
        lp.phi = (lp.phi < 0.) ? -M_HALFPI : M_HALFPI;
    else
        lp.phi *= 0.5;
    xy.x =
        0.5 * lp.lam *
        (cos(lp.phi) + static_cast<struct pj_wink2_data *>(P->opaque)->cosphi1);
    xy.y = M_FORTPI * (sin(lp.phi) + xy.y);
    return xy;
}

static PJ_LP wink2_s_inverse(PJ_XY xy, PJ *P) {
    PJ_LP lpInit;

    lpInit.phi = xy.y;
    lpInit.lam = xy.x;

    constexpr double deltaXYTolerance = 1e-10;
    return pj_generic_inverse_2d(xy, P, lpInit, deltaXYTolerance);
}

PJ *PJ_PROJECTION(wink2) {
    struct pj_wink2_data *Q = static_cast<struct pj_wink2_data *>(
        calloc(1, sizeof(struct pj_wink2_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    static_cast<struct pj_wink2_data *>(P->opaque)->cosphi1 =
        cos(pj_param(P->ctx, P->params, "rlat_1").f);
    P->es = 0.;
    P->fwd = wink2_s_forward;
    P->inv = wink2_s_inverse;

    return P;
}

#undef MAX_ITER
#undef LOOP_TOL
