
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(oea, "Oblated Equal Area") "\n\tMisc Sph\n\tn= m= theta=";

namespace { // anonymous namespace
struct pj_oea {
    double theta;
    double m, n;
    double two_r_m, two_r_n, rm, rn, hm, hn;
    double cp0, sp0;
};
} // anonymous namespace

static PJ_XY oea_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_oea *Q = static_cast<struct pj_oea *>(P->opaque);

    const double cp = cos(lp.phi);
    const double sp = sin(lp.phi);
    const double cl = cos(lp.lam);
    const double Az =
        aatan2(cp * sin(lp.lam), Q->cp0 * sp - Q->sp0 * cp * cl) + Q->theta;
    const double shz = sin(0.5 * aacos(P->ctx, Q->sp0 * sp + Q->cp0 * cp * cl));
    const double M = aasin(P->ctx, shz * sin(Az));
    const double N =
        aasin(P->ctx, shz * cos(Az) * cos(M) / cos(M * Q->two_r_m));
    xy.y = Q->n * sin(N * Q->two_r_n);
    xy.x = Q->m * sin(M * Q->two_r_m) * cos(N) / cos(N * Q->two_r_n);

    return xy;
}

static PJ_LP oea_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_oea *Q = static_cast<struct pj_oea *>(P->opaque);

    const double N = Q->hn * aasin(P->ctx, xy.y * Q->rn);
    const double M =
        Q->hm * aasin(P->ctx, xy.x * Q->rm * cos(N * Q->two_r_n) / cos(N));
    const double xp = 2. * sin(M);
    const double yp = 2. * sin(N) * cos(M * Q->two_r_m) / cos(M);
    const double Az = aatan2(xp, yp) - Q->theta;
    const double cAz = cos(Az);
    const double z = 2. * aasin(P->ctx, 0.5 * hypot(xp, yp));
    const double sz = sin(z);
    const double cz = cos(z);
    lp.phi = aasin(P->ctx, Q->sp0 * cz + Q->cp0 * sz * cAz);
    lp.lam = aatan2(sz * sin(Az), Q->cp0 * cz - Q->sp0 * sz * cAz);

    return lp;
}

PJ *PJ_PROJECTION(oea) {
    struct pj_oea *Q =
        static_cast<struct pj_oea *>(calloc(1, sizeof(struct pj_oea)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    if (((Q->n = pj_param(P->ctx, P->params, "dn").f) <= 0.)) {
        proj_log_error(P, _("Invalid value for n: it should be > 0"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    if (((Q->m = pj_param(P->ctx, P->params, "dm").f) <= 0.)) {
        proj_log_error(P, _("Invalid value for m: it should be > 0"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    Q->theta = pj_param(P->ctx, P->params, "rtheta").f;
    Q->sp0 = sin(P->phi0);
    Q->cp0 = cos(P->phi0);
    Q->rn = 1. / Q->n;
    Q->rm = 1. / Q->m;
    Q->two_r_n = 2. * Q->rn;
    Q->two_r_m = 2. * Q->rm;
    Q->hm = 0.5 * Q->m;
    Q->hn = 0.5 * Q->n;
    P->fwd = oea_s_forward;
    P->inv = oea_s_inverse;
    P->es = 0.;

    return P;
}
