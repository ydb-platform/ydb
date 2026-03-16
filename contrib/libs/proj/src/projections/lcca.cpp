/*****************************************************************************

               Lambert Conformal Conic Alternative
               -----------------------------------

    This is Gerald Evenden's 2003 implementation of an alternative
    "almost" LCC, which has been in use historically, but which
    should NOT be used for new projects - i.e: use this implementation
    if you need interoperability with old data represented in this
    projection, but not in any other case.

    The code was originally discussed on the PROJ.4 mailing list in
    a thread archived over at

    http://lists.maptools.org/pipermail/proj/2003-March/000644.html

    It was discussed again in the thread starting at

    http://lists.maptools.org/pipermail/proj/2017-October/007828.html
        and continuing at
    http://lists.maptools.org/pipermail/proj/2017-November/007831.html

    which prompted Clifford J. Mugnier to add these clarifying notes:

    The French Army Truncated Cubic Lambert (partially conformal) Conic
    projection is the Legal system for the projection in France between
    the late 1800s and 1948 when the French Legislature changed the law
    to recognize the fully conformal version.

    It was (might still be in one or two North African prior French
    Colonies) used in North Africa in Algeria, Tunisia, & Morocco, as
    well as in Syria during the Levant.

    Last time I have seen it used was about 30+ years ago in
    Algeria when it was used to define Lease Block boundaries for
    Petroleum Exploration & Production.

    (signed)

    Clifford J. Mugnier, c.p., c.m.s.
    Chief of Geodesy
    LSU Center for GeoInformatics
    Dept. of Civil Engineering
    LOUISIANA STATE UNIVERSITY

*****************************************************************************/

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(lcca, "Lambert Conformal Conic Alternative")
"\n\tConic, Sph&Ell\n\tlat_0=";

#define MAX_ITER 10
#define DEL_TOL 1e-12

namespace { // anonymous namespace
struct pj_lcca_data {
    double *en;
    double r0, l, M0;
    double C;
};
} // anonymous namespace

static double fS(double S, double C) { /* func to compute dr */

    return S * (1. + S * S * C);
}

static double fSp(double S, double C) { /* deriv of fs */

    return 1. + 3. * S * S * C;
}

static PJ_XY lcca_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_lcca_data *Q = static_cast<struct pj_lcca_data *>(P->opaque);
    double S, r, dr;

    S = pj_mlfn(lp.phi, sin(lp.phi), cos(lp.phi), Q->en) - Q->M0;
    dr = fS(S, Q->C);
    r = Q->r0 - dr;
    const double lam_mul_l = lp.lam * Q->l;
    xy.x = P->k0 * (r * sin(lam_mul_l));
    xy.y = P->k0 * (Q->r0 - r * cos(lam_mul_l));
    return xy;
}

static PJ_LP lcca_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_lcca_data *Q = static_cast<struct pj_lcca_data *>(P->opaque);
    double theta, dr, S, dif;
    int i;

    xy.x /= P->k0;
    xy.y /= P->k0;
    theta = atan2(xy.x, Q->r0 - xy.y);
    dr = xy.y - xy.x * tan(0.5 * theta);
    lp.lam = theta / Q->l;
    S = dr;
    for (i = MAX_ITER; i; --i) {
        S -= (dif = (fS(S, Q->C) - dr) / fSp(S, Q->C));
        if (fabs(dif) < DEL_TOL)
            break;
    }
    if (!i) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    lp.phi = pj_inv_mlfn(S + Q->M0, Q->en);

    return lp;
}

static PJ *pj_lcca_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_lcca_data *>(P->opaque)->en);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(lcca) {
    double s2p0, N0, R0, tan0;
    struct pj_lcca_data *Q = static_cast<struct pj_lcca_data *>(
        calloc(1, sizeof(struct pj_lcca_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    (Q->en = pj_enfn(P->n));
    if (!Q->en)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    if (P->phi0 == 0.) {
        proj_log_error(
            P, _("Invalid value for lat_0: it should be different from 0."));
        return pj_lcca_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    Q->l = sin(P->phi0);
    Q->M0 = pj_mlfn(P->phi0, Q->l, cos(P->phi0), Q->en);
    s2p0 = Q->l * Q->l;
    R0 = 1. / (1. - P->es * s2p0);
    N0 = sqrt(R0);
    R0 *= P->one_es * N0;
    tan0 = tan(P->phi0);
    Q->r0 = N0 / tan0;
    Q->C = 1. / (6. * R0 * N0);

    P->inv = lcca_e_inverse;
    P->fwd = lcca_e_forward;
    P->destructor = pj_lcca_destructor;

    return P;
}

#undef MAX_ITER
#undef DEL_TOL
