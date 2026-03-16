

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(kav5, "Kavrayskiy V") "\n\tPCyl, Sph";
PROJ_HEAD(qua_aut, "Quartic Authalic") "\n\tPCyl, Sph";
PROJ_HEAD(fouc, "Foucaut") "\n\tPCyl, Sph";
PROJ_HEAD(mbt_s, "McBryde-Thomas Flat-Polar Sine (No. 1)") "\n\tPCyl, Sph";

namespace { // anonymous namespace
struct pj_sts {
    double C_x, C_y, C_p;
    int tan_mode;
};
} // anonymous namespace

static PJ_XY sts_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_sts *Q = static_cast<struct pj_sts *>(P->opaque);

    xy.x = Q->C_x * lp.lam * cos(lp.phi);
    xy.y = Q->C_y;
    lp.phi *= Q->C_p;
    const double c = cos(lp.phi);
    if (Q->tan_mode) {
        xy.x *= c * c;
        xy.y *= tan(lp.phi);
    } else {
        xy.x /= c;
        xy.y *= sin(lp.phi);
    }
    return xy;
}

static PJ_LP sts_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_sts *Q = static_cast<struct pj_sts *>(P->opaque);

    xy.y /= Q->C_y;
    lp.phi = Q->tan_mode ? atan(xy.y) : aasin(P->ctx, xy.y);
    const double c = cos(lp.phi);
    lp.phi /= Q->C_p;
    lp.lam = xy.x / (Q->C_x * cos(lp.phi));
    if (Q->tan_mode)
        lp.lam /= c * c;
    else
        lp.lam *= c;
    return lp;
}

static PJ *setup(PJ *P, double p, double q, int mode) {
    P->es = 0.;
    P->inv = sts_s_inverse;
    P->fwd = sts_s_forward;
    static_cast<struct pj_sts *>(P->opaque)->C_x = q / p;
    static_cast<struct pj_sts *>(P->opaque)->C_y = p;
    static_cast<struct pj_sts *>(P->opaque)->C_p = 1 / q;
    static_cast<struct pj_sts *>(P->opaque)->tan_mode = mode;
    return P;
}

PJ *PJ_PROJECTION(fouc) {
    struct pj_sts *Q =
        static_cast<struct pj_sts *>(calloc(1, sizeof(struct pj_sts)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    return setup(P, 2., 2., 1);
}

PJ *PJ_PROJECTION(kav5) {
    struct pj_sts *Q =
        static_cast<struct pj_sts *>(calloc(1, sizeof(struct pj_sts)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    return setup(P, 1.50488, 1.35439, 0);
}

PJ *PJ_PROJECTION(qua_aut) {
    struct pj_sts *Q =
        static_cast<struct pj_sts *>(calloc(1, sizeof(struct pj_sts)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    return setup(P, 2., 2., 0);
}

PJ *PJ_PROJECTION(mbt_s) {
    struct pj_sts *Q =
        static_cast<struct pj_sts *>(calloc(1, sizeof(struct pj_sts)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    return setup(P, 1.48875, 1.36509, 0);
}
