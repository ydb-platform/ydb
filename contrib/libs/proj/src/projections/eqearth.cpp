/*
Equal Earth is a projection inspired by the Robinson projection, but unlike
the Robinson projection retains the relative size of areas. The projection
was designed in 2018 by Bojan Savric, Tom Patterson and Bernhard Jenny.

Publication:
Bojan Savric, Tom Patterson & Bernhard Jenny (2018). The Equal Earth map
projection, International Journal of Geographical Information Science,
DOI: 10.1080/13658816.2018.1504949

Port to PROJ by Juernjakob Dugge, 16 August 2018
Added ellipsoidal equations by Bojan Savric, 22 August 2018
*/

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(eqearth, "Equal Earth") "\n\tPCyl, Sph&Ell";

/* A1..A4, polynomial coefficients */
#define A1 1.340264
#define A2 -0.081106
#define A3 0.000893
#define A4 0.003796
#define M (sqrt(3.0) / 2.0)

#define MAX_Y 1.3173627591574 /* 90° latitude on a sphere with radius 1 */
#define EPS 1e-11
#define MAX_ITER 12

namespace { // anonymous namespace
struct pj_eqearth {
    double qp;
    double rqda;
    double *apa;
};
} // anonymous namespace

static PJ_XY eqearth_e_forward(PJ_LP lp,
                               PJ *P) { /* Ellipsoidal/spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_eqearth *Q = static_cast<struct pj_eqearth *>(P->opaque);
    double sbeta;
    double psi, psi2, psi6;

    /* Spheroidal case, using sine latitude */
    sbeta = sin(lp.phi);

    /* In the ellipsoidal case, we convert sbeta to sine of authalic latitude */
    if (P->es != 0.0) {
        sbeta = pj_authalic_lat_q(sbeta, P) / Q->qp;

        /* Rounding error. */
        if (fabs(sbeta) > 1)
            sbeta = sbeta > 0 ? 1 : -1;
    }

    /* Equal Earth projection */
    psi = asin(M * sbeta);
    psi2 = psi * psi;
    psi6 = psi2 * psi2 * psi2;

    xy.x = lp.lam * cos(psi) /
           (M * (A1 + 3 * A2 * psi2 + psi6 * (7 * A3 + 9 * A4 * psi2)));
    xy.y = psi * (A1 + A2 * psi2 + psi6 * (A3 + A4 * psi2));

    /* Adjusting x and y for authalic radius */
    xy.x *= Q->rqda;
    xy.y *= Q->rqda;

    return xy;
}

static PJ_LP eqearth_e_inverse(PJ_XY xy,
                               PJ *P) { /* Ellipsoidal/spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_eqearth *Q = static_cast<struct pj_eqearth *>(P->opaque);
    double yc, y2, y6;
    int i;

    /* Adjusting x and y for authalic radius */
    xy.x /= Q->rqda;
    xy.y /= Q->rqda;

    /* Make sure y is inside valid range */
    if (xy.y > MAX_Y)
        xy.y = MAX_Y;
    else if (xy.y < -MAX_Y)
        xy.y = -MAX_Y;

    yc = xy.y;

    /* Newton-Raphson */
    for (i = MAX_ITER; i; --i) {
        double f, fder, tol;

        y2 = yc * yc;
        y6 = y2 * y2 * y2;

        f = yc * (A1 + A2 * y2 + y6 * (A3 + A4 * y2)) - xy.y;
        fder = A1 + 3 * A2 * y2 + y6 * (7 * A3 + 9 * A4 * y2);

        tol = f / fder;
        yc -= tol;

        if (fabs(tol) < EPS)
            break;
    }

    if (i == 0) {
        proj_context_errno_set(
            P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }

    /* Longitude */
    y2 = yc * yc;
    y6 = y2 * y2 * y2;

    lp.lam =
        M * xy.x * (A1 + 3 * A2 * y2 + y6 * (7 * A3 + 9 * A4 * y2)) / cos(yc);

    /* Latitude (for spheroidal case, this is latitude */
    lp.phi = asin(sin(yc) / M);

    /* Ellipsoidal case, converting auth. latitude */
    if (P->es != 0.0)
        lp.phi = pj_authalic_lat_inverse(lp.phi, Q->apa, P, Q->qp);

    return lp;
}

static PJ *destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_eqearth *>(P->opaque)->apa);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(eqearth) {
    struct pj_eqearth *Q =
        static_cast<struct pj_eqearth *>(calloc(1, sizeof(struct pj_eqearth)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = destructor;
    P->fwd = eqearth_e_forward;
    P->inv = eqearth_e_inverse;
    Q->rqda = 1.0;

    /* Ellipsoidal case */
    if (P->es != 0.0) {
        Q->apa = pj_authalic_lat_compute_coeffs(P->n); /* For auth_lat(). */
        if (nullptr == Q->apa)
            return destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
        Q->qp = pj_authalic_lat_q(1.0, P); /* For auth_lat(). */
        Q->rqda = sqrt(0.5 * Q->qp); /* Authalic radius divided by major axis */
    }

    return P;
}

#undef A1
#undef A2
#undef A3
#undef A4
#undef M

#undef MAX_Y
#undef EPS
#undef MAX_ITER
