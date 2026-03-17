

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(loxim, "Loximuthal") "\n\tPCyl Sph";

#define EPS 1e-8

namespace { // anonymous namespace
struct pj_loxim_data {
    double phi1;
    double cosphi1;
    double tanphi1;
};
} // anonymous namespace

static PJ_XY loxim_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_loxim_data *Q = static_cast<struct pj_loxim_data *>(P->opaque);

    xy.y = lp.phi - Q->phi1;
    if (fabs(xy.y) < EPS)
        xy.x = lp.lam * Q->cosphi1;
    else {
        xy.x = M_FORTPI + 0.5 * lp.phi;
        if (fabs(xy.x) < EPS || fabs(fabs(xy.x) - M_HALFPI) < EPS)
            xy.x = 0.;
        else
            xy.x = lp.lam * xy.y / log(tan(xy.x) / Q->tanphi1);
    }
    return xy;
}

static PJ_LP loxim_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_loxim_data *Q = static_cast<struct pj_loxim_data *>(P->opaque);

    lp.phi = xy.y + Q->phi1;
    if (fabs(xy.y) < EPS) {
        lp.lam = xy.x / Q->cosphi1;
    } else {
        lp.lam = M_FORTPI + 0.5 * lp.phi;
        if (fabs(lp.lam) < EPS || fabs(fabs(lp.lam) - M_HALFPI) < EPS)
            lp.lam = 0.;
        else
            lp.lam = xy.x * log(tan(lp.lam) / Q->tanphi1) / xy.y;
    }
    return lp;
}

PJ *PJ_PROJECTION(loxim) {
    struct pj_loxim_data *Q = static_cast<struct pj_loxim_data *>(
        calloc(1, sizeof(struct pj_loxim_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->phi1 = pj_param(P->ctx, P->params, "rlat_1").f;
    Q->cosphi1 = cos(Q->phi1);
    if (Q->cosphi1 < EPS) {
        proj_log_error(P,
                       _("Invalid value for lat_1: |lat_1| should be < 90°"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    Q->tanphi1 = tan(M_FORTPI + 0.5 * Q->phi1);

    P->inv = loxim_s_inverse;
    P->fwd = loxim_s_forward;
    P->es = 0.;

    return P;
}

#undef EPS
