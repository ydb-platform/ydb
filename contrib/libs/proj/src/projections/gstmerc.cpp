

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(gstmerc,
          "Gauss-Schreiber Transverse Mercator (aka Gauss-Laborde Reunion)")
"\n\tCyl, Sph&Ell\n\tlat_0= lon_0= k_0=";

namespace { // anonymous namespace
struct pj_gstmerc_data {
    double lamc;
    double phic;
    double c;
    double n1;
    double n2;
    double XS;
    double YS;
};
} // anonymous namespace

static PJ_XY gstmerc_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_gstmerc_data *Q =
        static_cast<struct pj_gstmerc_data *>(P->opaque);
    double L, Ls, sinLs1, Ls1;

    L = Q->n1 * lp.lam;
    Ls = Q->c + Q->n1 * log(pj_tsfn(-lp.phi, -sin(lp.phi), P->e));
    sinLs1 = sin(L) / cosh(Ls);
    Ls1 = log(pj_tsfn(-asin(sinLs1), -sinLs1, 0.0));
    xy.x = (Q->XS + Q->n2 * Ls1) * P->ra;
    xy.y = (Q->YS + Q->n2 * atan(sinh(Ls) / cos(L))) * P->ra;

    return xy;
}

static PJ_LP gstmerc_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_gstmerc_data *Q =
        static_cast<struct pj_gstmerc_data *>(P->opaque);
    double L, LC, sinC;

    L = atan(sinh((xy.x * P->a - Q->XS) / Q->n2) /
             cos((xy.y * P->a - Q->YS) / Q->n2));
    sinC = sin((xy.y * P->a - Q->YS) / Q->n2) /
           cosh((xy.x * P->a - Q->XS) / Q->n2);
    LC = log(pj_tsfn(-asin(sinC), -sinC, 0.0));
    lp.lam = L / Q->n1;
    lp.phi = -pj_phi2(P->ctx, exp((LC - Q->c) / Q->n1), P->e);

    return lp;
}

PJ *PJ_PROJECTION(gstmerc) {
    struct pj_gstmerc_data *Q = static_cast<struct pj_gstmerc_data *>(
        calloc(1, sizeof(struct pj_gstmerc_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->lamc = P->lam0;
    Q->n1 = sqrt(1 + P->es * pow(cos(P->phi0), 4.0) / (1 - P->es));
    Q->phic = asin(sin(P->phi0) / Q->n1);
    Q->c = log(pj_tsfn(-Q->phic, -sin(P->phi0) / Q->n1, 0.0)) -
           Q->n1 * log(pj_tsfn(-P->phi0, -sin(P->phi0), P->e));
    Q->n2 = P->k0 * P->a * sqrt(1 - P->es) /
            (1 - P->es * sin(P->phi0) * sin(P->phi0));
    Q->XS = 0;
    Q->YS = -Q->n2 * Q->phic;

    P->inv = gstmerc_s_inverse;
    P->fwd = gstmerc_s_forward;

    return P;
}
