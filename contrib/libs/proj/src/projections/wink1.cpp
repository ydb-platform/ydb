

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(wink1, "Winkel I") "\n\tPCyl, Sph\n\tlat_ts=";

namespace { // anonymous namespace
struct pj_wink1_data {
    double cosphi1;
};
} // anonymous namespace

static PJ_XY wink1_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    xy.x =
        .5 * lp.lam *
        (static_cast<struct pj_wink1_data *>(P->opaque)->cosphi1 + cos(lp.phi));
    xy.y = lp.phi;
    return (xy);
}

static PJ_LP wink1_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    lp.phi = xy.y;
    lp.lam =
        2. * xy.x /
        (static_cast<struct pj_wink1_data *>(P->opaque)->cosphi1 + cos(lp.phi));
    return (lp);
}

PJ *PJ_PROJECTION(wink1) {
    struct pj_wink1_data *Q = static_cast<struct pj_wink1_data *>(
        calloc(1, sizeof(struct pj_wink1_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    static_cast<struct pj_wink1_data *>(P->opaque)->cosphi1 =
        cos(pj_param(P->ctx, P->params, "rlat_ts").f);
    P->es = 0.;
    P->inv = wink1_s_inverse;
    P->fwd = wink1_s_forward;

    return P;
}
