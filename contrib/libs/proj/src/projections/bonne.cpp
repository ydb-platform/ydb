
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(bonne, "Bonne (Werner lat_1=90)")
"\n\tConic Sph&Ell\n\tlat_1=";
#define EPS10 1e-10

namespace { // anonymous namespace
struct pj_bonne_data {
    double phi1;
    double cphi1;
    double am1;
    double m1;
    double *en;
};
} // anonymous namespace

static PJ_XY bonne_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_bonne_data *Q = static_cast<struct pj_bonne_data *>(P->opaque);
    double rh, E, c;

    E = sin(lp.phi);
    c = cos(lp.phi);
    rh = Q->am1 + Q->m1 - pj_mlfn(lp.phi, E, c, Q->en);
    if (fabs(rh) > EPS10) {
        E = c * lp.lam / (rh * sqrt(1. - P->es * E * E));
        xy.x = rh * sin(E);
        xy.y = Q->am1 - rh * cos(E);
    } else {
        xy.x = 0.;
        xy.y = 0.;
    }
    return xy;
}

static PJ_XY bonne_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_bonne_data *Q = static_cast<struct pj_bonne_data *>(P->opaque);
    double E, rh;

    rh = Q->cphi1 + Q->phi1 - lp.phi;
    if (fabs(rh) > EPS10) {
        E = lp.lam * cos(lp.phi) / rh;
        xy.x = rh * sin(E);
        xy.y = Q->cphi1 - rh * cos(E);
    } else
        xy.x = xy.y = 0.;
    return xy;
}

static PJ_LP bonne_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_bonne_data *Q = static_cast<struct pj_bonne_data *>(P->opaque);

    xy.y = Q->cphi1 - xy.y;
    const double rh = copysign(hypot(xy.x, xy.y), Q->phi1);
    lp.phi = Q->cphi1 + Q->phi1 - rh;
    const double abs_phi = fabs(lp.phi);
    if (abs_phi > M_HALFPI) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    if (M_HALFPI - abs_phi <= EPS10)
        lp.lam = 0.;
    else {
        const double lm = rh / cos(lp.phi);
        if (Q->phi1 > 0) {
            lp.lam = lm * atan2(xy.x, xy.y);
        } else {
            lp.lam = lm * atan2(-xy.x, -xy.y);
        }
    }
    return lp;
}

static PJ_LP bonne_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_bonne_data *Q = static_cast<struct pj_bonne_data *>(P->opaque);

    xy.y = Q->am1 - xy.y;
    const double rh = copysign(hypot(xy.x, xy.y), Q->phi1);
    lp.phi = pj_inv_mlfn(Q->am1 + Q->m1 - rh, Q->en);
    const double abs_phi = fabs(lp.phi);
    if (abs_phi < M_HALFPI) {
        const double sinphi = sin(lp.phi);
        const double lm = rh * sqrt(1. - P->es * sinphi * sinphi) / cos(lp.phi);
        if (Q->phi1 > 0) {
            lp.lam = lm * atan2(xy.x, xy.y);
        } else {
            lp.lam = lm * atan2(-xy.x, -xy.y);
        }
    } else if (abs_phi - M_HALFPI <= EPS10)
        lp.lam = 0.;
    else {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    return lp;
}

static PJ *pj_bonne_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_bonne_data *>(P->opaque)->en);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(bonne) {
    double c;
    struct pj_bonne_data *Q = static_cast<struct pj_bonne_data *>(
        calloc(1, sizeof(struct pj_bonne_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_bonne_destructor;

    Q->phi1 = pj_param(P->ctx, P->params, "rlat_1").f;
    if (fabs(Q->phi1) < EPS10) {
        proj_log_error(P, _("Invalid value for lat_1: |lat_1| should be > 0"));
        return pj_bonne_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    if (P->es != 0.0) {
        Q->en = pj_enfn(P->n);
        if (nullptr == Q->en)
            return pj_bonne_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
        Q->am1 = sin(Q->phi1);
        c = cos(Q->phi1);
        Q->m1 = pj_mlfn(Q->phi1, Q->am1, c, Q->en);
        Q->am1 = c / (sqrt(1. - P->es * Q->am1 * Q->am1) * Q->am1);
        P->inv = bonne_e_inverse;
        P->fwd = bonne_e_forward;
    } else {
        if (fabs(Q->phi1) + EPS10 >= M_HALFPI)
            Q->cphi1 = 0.;
        else
            Q->cphi1 = 1. / tan(Q->phi1);
        P->inv = bonne_s_inverse;
        P->fwd = bonne_s_forward;
    }
    return P;
}

#undef EPS10
