

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(cass, "Cassini") "\n\tCyl, Sph&Ell";

#define C1 .16666666666666666666
#define C2 .00833333333333333333
#define C3 .04166666666666666666
#define C4 .33333333333333333333
#define C5 .06666666666666666666

namespace { // anonymous namespace
struct cass_data {
    double *en;
    double m0;
    bool hyperbolic;
};
} // anonymous namespace

static PJ_XY cass_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct cass_data *Q = static_cast<struct cass_data *>(P->opaque);

    const double sinphi = sin(lp.phi);
    const double cosphi = cos(lp.phi);
    const double M = pj_mlfn(lp.phi, sinphi, cosphi, Q->en);

    const double nu_square = 1. / (1. - P->es * sinphi * sinphi);
    const double nu = sqrt(nu_square);
    const double tanphi = tan(lp.phi);
    const double T = tanphi * tanphi;
    const double A = lp.lam * cosphi;
    const double C = P->es * (cosphi * cosphi) / (1 - P->es);
    const double A2 = A * A;

    xy.x = nu * A * (1. - A2 * T * (C1 + (8. - T + 8. * C) * A2 * C2));
    xy.y = M - Q->m0 + nu * tanphi * A2 * (.5 + (5. - T + 6. * C) * A2 * C3);
    if (Q->hyperbolic) {
        const double rho = nu_square * (1. - P->es) * nu;
        xy.y -= xy.y * xy.y * xy.y / (6 * rho * nu);
    }

    return xy;
}

static PJ_XY cass_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    xy.x = asin(cos(lp.phi) * sin(lp.lam));
    xy.y = atan2(tan(lp.phi), cos(lp.lam)) - P->phi0;
    return xy;
}

static PJ_LP cass_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct cass_data *Q = static_cast<struct cass_data *>(P->opaque);

    const double phi1 = pj_inv_mlfn(Q->m0 + xy.y, Q->en);
    const double tanphi1 = tan(phi1);
    const double T1 = tanphi1 * tanphi1;
    const double sinphi1 = sin(phi1);
    const double nu1_square = 1. / (1. - P->es * sinphi1 * sinphi1);
    const double nu1 = sqrt(nu1_square);
    const double rho1 = nu1_square * (1. - P->es) * nu1;
    const double D = xy.x / nu1;
    const double D2 = D * D;
    lp.phi =
        phi1 - (nu1 * tanphi1 / rho1) * D2 * (.5 - (1. + 3. * T1) * D2 * C3);
    lp.lam = D * (1. + T1 * D2 * (-C4 + (1. + 3. * T1) * D2 * C5)) / cos(phi1);

    // EPSG guidance note 7-2 suggests a custom approximation for the
    // 'Vanua Levu 1915 / Vanua Levu Grid' case, but better use the
    // generic inversion method
    // Actually use it in the non-hyperbolic case. It enables to make the
    // 5108.gie roundtripping tests to success, with at most 2 iterations.
    constexpr double deltaXYTolerance = 1e-12;
    lp = pj_generic_inverse_2d(xy, P, lp, deltaXYTolerance);

    return lp;
}

static PJ_LP cass_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double dd;
    lp.phi = asin(sin(dd = xy.y + P->phi0) * cos(xy.x));
    lp.lam = atan2(tan(xy.x), cos(dd));
    return lp;
}

static PJ *pj_cass_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct cass_data *>(P->opaque)->en);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(cass) {

    /* Spheroidal? */
    if (0 == P->es) {
        P->inv = cass_s_inverse;
        P->fwd = cass_s_forward;
        return P;
    }

    /* otherwise it's ellipsoidal */
    auto Q =
        static_cast<struct cass_data *>(calloc(1, sizeof(struct cass_data)));
    P->opaque = Q;
    if (nullptr == P->opaque)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->destructor = pj_cass_destructor;

    Q->en = pj_enfn(P->n);
    if (nullptr == Q->en)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    Q->m0 = pj_mlfn(P->phi0, sin(P->phi0), cos(P->phi0), Q->en);
    if (pj_param_exists(P->params, "hyperbolic"))
        Q->hyperbolic = true;
    P->inv = cass_e_inverse;
    P->fwd = cass_e_forward;

    return P;
}

#undef C1
#undef C2
#undef C3
#undef C4
#undef C5
