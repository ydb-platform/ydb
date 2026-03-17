

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(wag3, "Wagner III") "\n\tPCyl, Sph\n\tlat_ts=";

#define TWOTHIRD 0.6666666666666666666667

namespace { // anonymous namespace
struct pj_wag3 {
    double C_x;
};
} // anonymous namespace

static PJ_XY wag3_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    xy.x = static_cast<struct pj_wag3 *>(P->opaque)->C_x * lp.lam *
           cos(TWOTHIRD * lp.phi);
    xy.y = lp.phi;
    return xy;
}

static PJ_LP wag3_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    lp.phi = xy.y;
    lp.lam = xy.x / (static_cast<struct pj_wag3 *>(P->opaque)->C_x *
                     cos(TWOTHIRD * lp.phi));
    return lp;
}

PJ *PJ_PROJECTION(wag3) {
    double ts;
    struct pj_wag3 *Q =
        static_cast<struct pj_wag3 *>(calloc(1, sizeof(struct pj_wag3)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    P->opaque = Q;

    ts = pj_param(P->ctx, P->params, "rlat_ts").f;
    static_cast<struct pj_wag3 *>(P->opaque)->C_x = cos(ts) / cos(2. * ts / 3.);
    P->es = 0.;
    P->inv = wag3_s_inverse;
    P->fwd = wag3_s_forward;

    return P;
}

#undef TWOTHIRD
