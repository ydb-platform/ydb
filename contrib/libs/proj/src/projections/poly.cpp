

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(poly, "Polyconic (American)")
"\n\tConic, Sph&Ell";

namespace { // anonymous namespace
struct pj_poly_data {
    double ml0;
    double *en;
};
} // anonymous namespace

#define TOL 1e-10
#define CONV 1e-10
#define N_ITER 10
#define I_ITER 20
#define ITOL 1.e-12

static PJ_XY poly_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_poly_data *Q = static_cast<struct pj_poly_data *>(P->opaque);
    double ms, sp, cp;

    if (fabs(lp.phi) <= TOL) {
        xy.x = lp.lam;
        xy.y = -Q->ml0;
    } else {
        sp = sin(lp.phi);
        cp = cos(lp.phi);
        ms = fabs(cp) > TOL ? pj_msfn(sp, cp, P->es) / sp : 0.;
        lp.lam *= sp;
        xy.x = ms * sin(lp.lam);
        xy.y =
            (pj_mlfn(lp.phi, sp, cp, Q->en) - Q->ml0) + ms * (1. - cos(lp.lam));
    }

    return xy;
}

static PJ_XY poly_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_poly_data *Q = static_cast<struct pj_poly_data *>(P->opaque);

    if (fabs(lp.phi) <= TOL) {
        xy.x = lp.lam;
        xy.y = Q->ml0;
    } else {
        const double cot = 1. / tan(lp.phi);
        const double E = lp.lam * sin(lp.phi);
        xy.x = sin(E) * cot;
        xy.y = lp.phi - P->phi0 + cot * (1. - cos(E));
    }

    return xy;
}

static PJ_LP poly_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_poly_data *Q = static_cast<struct pj_poly_data *>(P->opaque);

    xy.y += Q->ml0;
    if (fabs(xy.y) <= TOL) {
        lp.lam = xy.x;
        lp.phi = 0.;
    } else {
        int i;

        const double r = xy.y * xy.y + xy.x * xy.x;
        lp.phi = xy.y;
        for (i = I_ITER; i; --i) {
            const double sp = sin(lp.phi);
            const double cp = cos(lp.phi);
            const double s2ph = sp * cp;
            if (fabs(cp) < ITOL) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return lp;
            }
            double mlp = sqrt(1. - P->es * sp * sp);
            const double c = sp * mlp / cp;
            const double ml = pj_mlfn(lp.phi, sp, cp, Q->en);
            const double mlb = ml * ml + r;
            mlp = P->one_es / (mlp * mlp * mlp);
            const double dPhi =
                (ml + ml + c * mlb - 2. * xy.y * (c * ml + 1.)) /
                (P->es * s2ph * (mlb - 2. * xy.y * ml) / c +
                 2. * (xy.y - ml) * (c * mlp - 1. / s2ph) - mlp - mlp);
            lp.phi += dPhi;
            if (fabs(dPhi) <= ITOL)
                break;
        }
        if (!i) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        }
        const double c = sin(lp.phi);
        lp.lam =
            asin(xy.x * tan(lp.phi) * sqrt(1. - P->es * c * c)) / sin(lp.phi);
    }

    return lp;
}

static PJ_LP poly_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};

    if (fabs(xy.y = P->phi0 + xy.y) <= TOL) {
        lp.lam = xy.x;
        lp.phi = 0.;
    } else {
        lp.phi = xy.y;
        const double B = xy.x * xy.x + xy.y * xy.y;
        int i = N_ITER;
        while (true) {
            const double tp = tan(lp.phi);
            const double dphi = (xy.y * (lp.phi * tp + 1.) - lp.phi -
                                 .5 * (lp.phi * lp.phi + B) * tp) /
                                ((lp.phi - xy.y) / tp - 1.);
            lp.phi -= dphi;
            if (!(fabs(dphi) > CONV))
                break;
            --i;
            if (i == 0) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return lp;
            }
        }
        lp.lam = asin(xy.x * tan(lp.phi)) / sin(lp.phi);
    }

    return lp;
}

static PJ *pj_poly_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    if (static_cast<struct pj_poly_data *>(P->opaque)->en)
        free(static_cast<struct pj_poly_data *>(P->opaque)->en);

    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(poly) {
    struct pj_poly_data *Q = static_cast<struct pj_poly_data *>(
        calloc(1, sizeof(struct pj_poly_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    P->opaque = Q;
    P->destructor = pj_poly_destructor;

    if (P->es != 0.0) {
        if (!(Q->en = pj_enfn(P->n)))
            return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
        Q->ml0 = pj_mlfn(P->phi0, sin(P->phi0), cos(P->phi0), Q->en);
        P->inv = poly_e_inverse;
        P->fwd = poly_e_forward;
    } else {
        Q->ml0 = -P->phi0;
        P->inv = poly_s_inverse;
        P->fwd = poly_s_forward;
    }

    return P;
}

#undef TOL
#undef CONV
#undef N_ITER
#undef I_ITER
#undef ITOL
