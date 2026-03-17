

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

namespace { // anonymous namespace
struct pj_eqc_data {
    double rc;
};
} // anonymous namespace

PROJ_HEAD(eqc, "Equidistant Cylindrical (Plate Carree)")
"\n\tCyl, Sph\n\tlat_ts=[, lat_0=0]";

static PJ_XY eqc_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_eqc_data *Q = static_cast<struct pj_eqc_data *>(P->opaque);

    xy.x = Q->rc * lp.lam;
    xy.y = lp.phi - P->phi0;

    return xy;
}

static PJ_LP eqc_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_eqc_data *Q = static_cast<struct pj_eqc_data *>(P->opaque);

    lp.lam = xy.x / Q->rc;
    lp.phi = xy.y + P->phi0;

    return lp;
}

PJ *PJ_PROJECTION(eqc) {
    struct pj_eqc_data *Q = static_cast<struct pj_eqc_data *>(
        calloc(1, sizeof(struct pj_eqc_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    if ((Q->rc = cos(pj_param(P->ctx, P->params, "rlat_ts").f)) <= 0.) {
        proj_log_error(
            P, _("Invalid value for lat_ts: |lat_ts| should be <= 90°"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    P->inv = eqc_s_inverse;
    P->fwd = eqc_s_forward;
    P->es = 0.;

    return P;
}
