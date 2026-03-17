

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(urm5, "Urmaev V") "\n\tPCyl, Sph, no inv\n\tn= q= alpha=";

namespace { // anonymous namespace
struct pj_urm5 {
    double m, rmn, q3, n;
};
} // anonymous namespace

static PJ_XY urm5_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_urm5 *Q = static_cast<struct pj_urm5 *>(P->opaque);
    double t;

    t = lp.phi = aasin(P->ctx, Q->n * sin(lp.phi));
    xy.x = Q->m * lp.lam * cos(lp.phi);
    t *= t;
    xy.y = lp.phi * (1. + t * Q->q3) * Q->rmn;
    return xy;
}

PJ *PJ_PROJECTION(urm5) {
    double alpha, t;
    struct pj_urm5 *Q =
        static_cast<struct pj_urm5 *>(calloc(1, sizeof(struct pj_urm5)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    if (!pj_param(P->ctx, P->params, "tn").i) {
        proj_log_error(P, _("Missing parameter n."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    Q->n = pj_param(P->ctx, P->params, "dn").f;
    if (Q->n <= 0. || Q->n > 1.) {
        proj_log_error(P,
                       _("Invalid value for n: it should be in ]0,1] range."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    Q->q3 = pj_param(P->ctx, P->params, "dq").f / 3.;
    alpha = pj_param(P->ctx, P->params, "ralpha").f;
    t = Q->n * sin(alpha);
    const double denom = sqrt(1. - t * t);
    if (denom == 0) {
        proj_log_error(
            P,
            _("Invalid value for n / alpha: n * sin(|alpha|) should be < 1."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    Q->m = cos(alpha) / denom;
    Q->rmn = 1. / (Q->m * Q->n);

    P->es = 0.;
    P->inv = nullptr;
    P->fwd = urm5_s_forward;

    return P;
}
