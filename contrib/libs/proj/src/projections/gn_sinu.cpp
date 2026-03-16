

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(gn_sinu, "General Sinusoidal Series") "\n\tPCyl, Sph\n\tm= n=";
PROJ_HEAD(sinu, "Sinusoidal (Sanson-Flamsteed)") "\n\tPCyl, Sph&Ell";
PROJ_HEAD(eck6, "Eckert VI") "\n\tPCyl, Sph";
PROJ_HEAD(mbtfps, "McBryde-Thomas Flat-Polar Sinusoidal") "\n\tPCyl, Sph";

#define EPS10 1e-10
#define MAX_ITER 8
#define LOOP_TOL 1e-7

namespace { // anonymous namespace
struct pj_gn_sinu_data {
    double *en;
    double m, n, C_x, C_y;
};
} // anonymous namespace

static PJ_XY gn_sinu_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};

    const double s = sin(lp.phi);
    const double c = cos(lp.phi);
    xy.y = pj_mlfn(lp.phi, s, c,
                   static_cast<struct pj_gn_sinu_data *>(P->opaque)->en);
    xy.x = lp.lam * c / sqrt(1. - P->es * s * s);
    return xy;
}

static PJ_LP gn_sinu_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double s;

    lp.phi =
        pj_inv_mlfn(xy.y, static_cast<struct pj_gn_sinu_data *>(P->opaque)->en);
    s = fabs(lp.phi);
    if (s < M_HALFPI) {
        s = sin(lp.phi);
        lp.lam = xy.x * sqrt(1. - P->es * s * s) / cos(lp.phi);
    } else if ((s - EPS10) < M_HALFPI) {
        lp.lam = 0.;
    } else {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    }

    return lp;
}

static PJ_XY gn_sinu_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_gn_sinu_data *Q =
        static_cast<struct pj_gn_sinu_data *>(P->opaque);

    if (Q->m == 0.0)
        lp.phi = Q->n != 1. ? aasin(P->ctx, Q->n * sin(lp.phi)) : lp.phi;
    else {
        int i;

        const double k = Q->n * sin(lp.phi);
        for (i = MAX_ITER; i; --i) {
            const double V =
                (Q->m * lp.phi + sin(lp.phi) - k) / (Q->m + cos(lp.phi));
            lp.phi -= V;
            if (fabs(V) < LOOP_TOL)
                break;
        }
        if (!i) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
    }
    xy.x = Q->C_x * lp.lam * (Q->m + cos(lp.phi));
    xy.y = Q->C_y * lp.phi;

    return xy;
}

static PJ_LP gn_sinu_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_gn_sinu_data *Q =
        static_cast<struct pj_gn_sinu_data *>(P->opaque);

    xy.y /= Q->C_y;
    lp.phi = (Q->m != 0.0)
                 ? aasin(P->ctx, (Q->m * xy.y + sin(xy.y)) / Q->n)
                 : (Q->n != 1. ? aasin(P->ctx, sin(xy.y) / Q->n) : xy.y);
    lp.lam = xy.x / (Q->C_x * (Q->m + cos(xy.y)));
    return lp;
}

static PJ *pj_gn_sinu_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_gn_sinu_data *>(P->opaque)->en);
    return pj_default_destructor(P, errlev);
}

/* for spheres, only */
static void pj_gn_sinu_setup(PJ *P) {
    struct pj_gn_sinu_data *Q =
        static_cast<struct pj_gn_sinu_data *>(P->opaque);
    P->es = 0;
    P->inv = gn_sinu_s_inverse;
    P->fwd = gn_sinu_s_forward;

    Q->C_y = sqrt((Q->m + 1.) / Q->n);
    Q->C_x = Q->C_y / (Q->m + 1.);
}

PJ *PJ_PROJECTION(sinu) {
    struct pj_gn_sinu_data *Q = static_cast<struct pj_gn_sinu_data *>(
        calloc(1, sizeof(struct pj_gn_sinu_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_gn_sinu_destructor;

    if (!(Q->en = pj_enfn(P->n)))
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    if (P->es != 0.0) {
        P->inv = gn_sinu_e_inverse;
        P->fwd = gn_sinu_e_forward;
    } else {
        Q->n = 1.;
        Q->m = 0.;
        pj_gn_sinu_setup(P);
    }
    return P;
}

PJ *PJ_PROJECTION(eck6) {
    struct pj_gn_sinu_data *Q = static_cast<struct pj_gn_sinu_data *>(
        calloc(1, sizeof(struct pj_gn_sinu_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_gn_sinu_destructor;

    Q->m = 1.;
    Q->n = 2.570796326794896619231321691;
    pj_gn_sinu_setup(P);

    return P;
}

PJ *PJ_PROJECTION(mbtfps) {
    struct pj_gn_sinu_data *Q = static_cast<struct pj_gn_sinu_data *>(
        calloc(1, sizeof(struct pj_gn_sinu_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_gn_sinu_destructor;

    Q->m = 0.5;
    Q->n = 1.785398163397448309615660845;
    pj_gn_sinu_setup(P);

    return P;
}

PJ *PJ_PROJECTION(gn_sinu) {
    struct pj_gn_sinu_data *Q = static_cast<struct pj_gn_sinu_data *>(
        calloc(1, sizeof(struct pj_gn_sinu_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_gn_sinu_destructor;

    if (!pj_param(P->ctx, P->params, "tn").i) {
        proj_log_error(P, _("Missing parameter n."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    if (!pj_param(P->ctx, P->params, "tm").i) {
        proj_log_error(P, _("Missing parameter m."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    Q->n = pj_param(P->ctx, P->params, "dn").f;
    Q->m = pj_param(P->ctx, P->params, "dm").f;
    if (Q->n <= 0) {
        proj_log_error(P, _("Invalid value for n: it should be > 0."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (Q->m < 0) {
        proj_log_error(P, _("Invalid value for m: it should be >= 0."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    pj_gn_sinu_setup(P);

    return P;
}
