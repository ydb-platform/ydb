

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(moll, "Mollweide") "\n\tPCyl, Sph";
PROJ_HEAD(wag4, "Wagner IV") "\n\tPCyl, Sph";
PROJ_HEAD(wag5, "Wagner V") "\n\tPCyl, Sph";

#define MAX_ITER 30
#define LOOP_TOL 1e-7

namespace { // anonymous namespace
struct pj_moll_data {
    double C_x, C_y, C_p;
};
} // anonymous namespace

static PJ_XY moll_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_moll_data *Q = static_cast<struct pj_moll_data *>(P->opaque);
    int i;

    const double k = Q->C_p * sin(lp.phi);
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
    xy.x = Q->C_x * lp.lam * cos(lp.phi);
    xy.y = Q->C_y * sin(lp.phi);
    return xy;
}

static PJ_LP moll_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_moll_data *Q = static_cast<struct pj_moll_data *>(P->opaque);
    lp.phi = aasin(P->ctx, xy.y / Q->C_y);
    lp.lam = xy.x / (Q->C_x * cos(lp.phi));
    if (fabs(lp.lam) < M_PI) {
        lp.phi += lp.phi;
        lp.phi = aasin(P->ctx, (lp.phi + sin(lp.phi)) / Q->C_p);
    } else {
        lp.lam = lp.phi = HUGE_VAL;
    }
    return lp;
}

static PJ *setup(PJ *P, double p) {
    struct pj_moll_data *Q = static_cast<struct pj_moll_data *>(P->opaque);
    double r, sp, p2 = p + p;

    P->es = 0;
    sp = sin(p);
    r = sqrt(M_TWOPI * sp / (p2 + sin(p2)));

    Q->C_x = 2. * r / M_PI;
    Q->C_y = r / sp;
    Q->C_p = p2 + sin(p2);

    P->inv = moll_s_inverse;
    P->fwd = moll_s_forward;
    return P;
}

PJ *PJ_PROJECTION(moll) {
    struct pj_moll_data *Q = static_cast<struct pj_moll_data *>(
        calloc(1, sizeof(struct pj_moll_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    return setup(P, M_HALFPI);
}

PJ *PJ_PROJECTION(wag4) {
    struct pj_moll_data *Q = static_cast<struct pj_moll_data *>(
        calloc(1, sizeof(struct pj_moll_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    return setup(P, M_PI / 3.);
}

PJ *PJ_PROJECTION(wag5) {
    struct pj_moll_data *Q = static_cast<struct pj_moll_data *>(
        calloc(1, sizeof(struct pj_moll_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    P->es = 0;
    Q->C_x = 0.90977;
    Q->C_y = 1.65014;
    Q->C_p = 3.00896;

    P->inv = moll_s_inverse;
    P->fwd = moll_s_forward;

    return P;
}

#undef MAX_ITER
#undef LOOP_TOL
