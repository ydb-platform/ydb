

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(eck3, "Eckert III") "\n\tPCyl, Sph";
PROJ_HEAD(putp1, "Putnins P1") "\n\tPCyl, Sph";
PROJ_HEAD(wag6, "Wagner VI") "\n\tPCyl, Sph";
PROJ_HEAD(kav7, "Kavrayskiy VII") "\n\tPCyl, Sph";

namespace { // anonymous namespace
struct pj_opaque {
    double C_x, C_y, A, B;
};
} // anonymous namespace

static PJ_XY eck3_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque *>(P->opaque);

    xy.y = Q->C_y * lp.phi;
    xy.x = Q->C_x * lp.lam * (Q->A + asqrt(1. - Q->B * lp.phi * lp.phi));
    return xy;
}

static PJ_LP eck3_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque *>(P->opaque);
    double denominator;

    lp.phi = xy.y / Q->C_y;
    denominator = (Q->C_x * (Q->A + asqrt(1. - Q->B * lp.phi * lp.phi)));
    if (denominator == 0.0)
        lp.lam = HUGE_VAL;
    else
        lp.lam = xy.x / denominator;
    return lp;
}

static PJ *setup(PJ *P) {
    P->es = 0.;
    P->inv = eck3_s_inverse;
    P->fwd = eck3_s_forward;
    return P;
}

PJ *PJ_PROJECTION(eck3) {
    struct pj_opaque *Q =
        static_cast<struct pj_opaque *>(calloc(1, sizeof(struct pj_opaque)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->C_x = 0.42223820031577120149;
    Q->C_y = 0.84447640063154240298;
    Q->A = 1.0;
    Q->B = 0.4052847345693510857755;

    return setup(P);
}

PJ *PJ_PROJECTION(kav7) {
    struct pj_opaque *Q =
        static_cast<struct pj_opaque *>(calloc(1, sizeof(struct pj_opaque)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    /* Defined twice in original code - Using 0.866...,
     * but leaving the other one here as a safety measure.
     * Q->C_x = 0.2632401569273184856851; */
    Q->C_x = 0.8660254037844;
    Q->C_y = 1.;
    Q->A = 0.;
    Q->B = 0.30396355092701331433;

    return setup(P);
}

PJ *PJ_PROJECTION(wag6) {
    struct pj_opaque *Q =
        static_cast<struct pj_opaque *>(calloc(1, sizeof(struct pj_opaque)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->C_x = 1.0;
    Q->C_y = 1.0;
    Q->A = 0.0;
    Q->B = 0.30396355092701331433;

    return setup(P);
}

PJ *PJ_PROJECTION(putp1) {
    struct pj_opaque *Q =
        static_cast<struct pj_opaque *>(calloc(1, sizeof(struct pj_opaque)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->C_x = 1.89490;
    Q->C_y = 0.94745;
    Q->A = -0.5;
    Q->B = 0.30396355092701331433;

    return setup(P);
}
