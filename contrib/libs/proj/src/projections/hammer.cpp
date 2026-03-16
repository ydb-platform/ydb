

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(hammer, "Hammer & Eckert-Greifendorff")
"\n\tMisc Sph, \n\tW= M=";

#define EPS 1.0e-10

namespace { // anonymous namespace
struct pq_hammer {
    double w;
    double m, rm;
};
} // anonymous namespace

static PJ_XY hammer_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pq_hammer *Q = static_cast<struct pq_hammer *>(P->opaque);
    double cosphi, d;

    cosphi = cos(lp.phi);
    lp.lam *= Q->w;
    double denom = 1. + cosphi * cos(lp.lam);
    if (denom == 0.0) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().xy;
    }
    d = sqrt(2. / denom);
    xy.x = Q->m * d * cosphi * sin(lp.lam);
    xy.y = Q->rm * d * sin(lp.phi);
    return xy;
}

static PJ_LP hammer_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pq_hammer *Q = static_cast<struct pq_hammer *>(P->opaque);
    double z;

    z = sqrt(1. - 0.25 * Q->w * Q->w * xy.x * xy.x - 0.25 * xy.y * xy.y);
    if (fabs(2. * z * z - 1.) < EPS) {
        lp.lam = HUGE_VAL;
        lp.phi = HUGE_VAL;
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    } else {
        lp.lam = aatan2(Q->w * xy.x * z, 2. * z * z - 1) / Q->w;
        lp.phi = aasin(P->ctx, z * xy.y);
    }
    return lp;
}

PJ *PJ_PROJECTION(hammer) {
    struct pq_hammer *Q =
        static_cast<struct pq_hammer *>(calloc(1, sizeof(struct pq_hammer)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    if (pj_param(P->ctx, P->params, "tW").i) {
        Q->w = fabs(pj_param(P->ctx, P->params, "dW").f);
        if (Q->w <= 0.) {
            proj_log_error(P, _("Invalid value for W: it should be > 0"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    } else
        Q->w = .5;
    if (pj_param(P->ctx, P->params, "tM").i) {
        Q->m = fabs(pj_param(P->ctx, P->params, "dM").f);
        if (Q->m <= 0.) {
            proj_log_error(P, _("Invalid value for M: it should be > 0"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    } else
        Q->m = 1.;

    Q->rm = 1. / Q->m;
    Q->m /= Q->w;

    P->es = 0.;
    P->fwd = hammer_s_forward;
    P->inv = hammer_s_inverse;

    return P;
}

#undef EPS
