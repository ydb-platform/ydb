

#include <float.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"
#include <math.h>

PROJ_HEAD(tobmerc, "Tobler-Mercator") "\n\tCyl, Sph";

static PJ_XY tobmerc_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double cosphi;

    if (fabs(lp.phi) >= M_HALFPI) {
        // builtins.gie tests "Test expected failure at the poles:".  However
        // given that M_HALFPI is strictly less than pi/2 in double precision,
        // it's not clear why shouldn't just return a large result for xy.y (and
        // it's not even that large, merely 38.025...).  Even if the logic was
        // such that phi was strictly equal to pi/2, allowing xy.y = inf would
        // be a reasonable result.
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }

    cosphi = cos(lp.phi);
    xy.x = P->k0 * lp.lam * cosphi * cosphi;
    xy.y = P->k0 * asinh(tan(lp.phi));
    return xy;
}

static PJ_LP tobmerc_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double cosphi;

    lp.phi = atan(sinh(xy.y / P->k0));
    cosphi = cos(lp.phi);
    lp.lam = xy.x / P->k0 / (cosphi * cosphi);
    return lp;
}

PJ *PJ_PROJECTION(tobmerc) {
    P->inv = tobmerc_s_inverse;
    P->fwd = tobmerc_s_forward;
    return P;
}
