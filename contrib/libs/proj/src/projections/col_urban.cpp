

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(col_urban, "Colombia Urban")
"\n\tMisc\n\th_0=";

// Notations and formulas taken from IOGP Publication 373-7-2 -
// Geomatics Guidance Note number 7, part 2 - March 2020

namespace { // anonymous namespace

struct pj_col_urban {
    double h0;   // height of projection origin, divided by semi-major axis (a)
    double rho0; // adimensional value, contrary to Guidance note 7.2
    double A;
    double B; // adimensional value, contrary to Guidance note 7.2
    double C;
    double D; // adimensional value, contrary to Guidance note 7.2
};
} // anonymous namespace

static PJ_XY col_urban_forward(PJ_LP lp, PJ *P) {
    PJ_XY xy;
    struct pj_col_urban *Q = static_cast<struct pj_col_urban *>(P->opaque);

    const double cosphi = cos(lp.phi);
    const double sinphi = sin(lp.phi);
    const double nu = 1. / sqrt(1 - P->es * sinphi * sinphi);
    const double lam_nu_cosphi = lp.lam * nu * cosphi;
    xy.x = Q->A * lam_nu_cosphi;
    const double sinphi_m = sin(0.5 * (lp.phi + P->phi0));
    const double rho_m =
        (1 - P->es) / pow(1 - P->es * sinphi_m * sinphi_m, 1.5);
    const double G = 1 + Q->h0 / rho_m;
    xy.y = G * Q->rho0 *
           ((lp.phi - P->phi0) + Q->B * lam_nu_cosphi * lam_nu_cosphi);

    return xy;
}

static PJ_LP col_urban_inverse(PJ_XY xy, PJ *P) {
    PJ_LP lp;
    struct pj_col_urban *Q = static_cast<struct pj_col_urban *>(P->opaque);

    lp.phi = P->phi0 + xy.y / Q->D - Q->B * (xy.x / Q->C) * (xy.x / Q->C);
    const double sinphi = sin(lp.phi);
    const double nu = 1. / sqrt(1 - P->es * sinphi * sinphi);
    lp.lam = xy.x / (Q->C * nu * cos(lp.phi));

    return lp;
}

PJ *PJ_PROJECTION(col_urban) {
    struct pj_col_urban *Q = static_cast<struct pj_col_urban *>(
        calloc(1, sizeof(struct pj_col_urban)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    const double h0_unscaled = pj_param(P->ctx, P->params, "dh_0").f;
    Q->h0 = h0_unscaled / P->a;
    const double sinphi0 = sin(P->phi0);
    const double nu0 = 1. / sqrt(1 - P->es * sinphi0 * sinphi0);
    Q->A = 1 + Q->h0 / nu0;
    Q->rho0 = (1 - P->es) / pow(1 - P->es * sinphi0 * sinphi0, 1.5);
    Q->B = tan(P->phi0) / (2 * Q->rho0 * nu0);
    Q->C = 1 + Q->h0;
    Q->D = Q->rho0 * (1 + Q->h0 / (1 - P->es));

    P->fwd = col_urban_forward;
    P->inv = col_urban_inverse;

    return P;
}
