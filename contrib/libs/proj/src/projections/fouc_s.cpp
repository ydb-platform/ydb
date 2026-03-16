

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(fouc_s, "Foucaut Sinusoidal") "\n\tPCyl, Sph";

#define MAX_ITER 10
#define LOOP_TOL 1e-7

namespace { // anonymous namespace
struct pj_fouc_s_data {
    double n, n1;
};
} // anonymous namespace

static PJ_XY fouc_s_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_fouc_s_data *Q = static_cast<struct pj_fouc_s_data *>(P->opaque);
    double t;

    t = cos(lp.phi);
    xy.x = lp.lam * t / (Q->n + Q->n1 * t);
    xy.y = Q->n * lp.phi + Q->n1 * sin(lp.phi);
    return xy;
}

static PJ_LP fouc_s_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_fouc_s_data *Q = static_cast<struct pj_fouc_s_data *>(P->opaque);
    int i;

    if (Q->n != 0.0) {
        lp.phi = xy.y;
        for (i = MAX_ITER; i; --i) {
            const double V = (Q->n * lp.phi + Q->n1 * sin(lp.phi) - xy.y) /
                             (Q->n + Q->n1 * cos(lp.phi));
            lp.phi -= V;
            if (fabs(V) < LOOP_TOL)
                break;
        }
        if (!i)
            lp.phi = xy.y < 0. ? -M_HALFPI : M_HALFPI;
    } else
        lp.phi = aasin(P->ctx, xy.y);
    const double V = cos(lp.phi);
    lp.lam = xy.x * (Q->n + Q->n1 * V) / V;
    return lp;
}

PJ *PJ_PROJECTION(fouc_s) {
    struct pj_fouc_s_data *Q = static_cast<struct pj_fouc_s_data *>(
        calloc(1, sizeof(struct pj_fouc_s_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->n = pj_param(P->ctx, P->params, "dn").f;
    if (Q->n < 0. || Q->n > 1.) {
        proj_log_error(P,
                       _("Invalid value for n: it should be in [0,1] range."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    Q->n1 = 1. - Q->n;
    P->es = 0;
    P->inv = fouc_s_s_inverse;
    P->fwd = fouc_s_s_forward;
    return P;
}

#undef MAX_ITER
#undef LOOP_TOL
