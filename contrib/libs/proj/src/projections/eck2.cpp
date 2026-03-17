

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(eck2, "Eckert II") "\n\tPCyl, Sph";

#define FXC 0.46065886596178063902
#define FYC 1.44720250911653531871
#define C13 0.33333333333333333333
#define ONEEPS 1.0000001

static PJ_XY eck2_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    xy.x = FXC * lp.lam * (xy.y = sqrt(4. - 3. * sin(fabs(lp.phi))));
    xy.y = FYC * (2. - xy.y);
    if (lp.phi < 0.)
        xy.y = -xy.y;

    return (xy);
}

static PJ_LP eck2_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    (void)P;

    lp.phi = 2. - fabs(xy.y) / FYC;
    lp.lam = xy.x / (FXC * lp.phi);
    lp.phi = (4. - lp.phi * lp.phi) * C13;
    if (fabs(lp.phi) >= 1.) {
        if (fabs(lp.phi) > ONEEPS) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else {
            lp.phi = lp.phi < 0. ? -M_HALFPI : M_HALFPI;
        }
    } else
        lp.phi = asin(lp.phi);
    if (xy.y < 0)
        lp.phi = -lp.phi;
    return (lp);
}

PJ *PJ_PROJECTION(eck2) {
    P->es = 0.;
    P->inv = eck2_s_inverse;
    P->fwd = eck2_s_forward;

    return P;
}
