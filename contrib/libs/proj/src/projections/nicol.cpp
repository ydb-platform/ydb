

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(nicol, "Nicolosi Globular") "\n\tMisc Sph, no inv";

#define EPS 1e-10

static PJ_XY nicol_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    if (fabs(lp.lam) < EPS) {
        xy.x = 0;
        xy.y = lp.phi;
    } else if (fabs(lp.phi) < EPS) {
        xy.x = lp.lam;
        xy.y = 0.;
    } else if (fabs(fabs(lp.lam) - M_HALFPI) < EPS) {
        xy.x = lp.lam * cos(lp.phi);
        xy.y = M_HALFPI * sin(lp.phi);
    } else if (fabs(fabs(lp.phi) - M_HALFPI) < EPS) {
        xy.x = 0;
        xy.y = lp.phi;
    } else {
        double tb, c, d, m, n, r2, sp;

        tb = M_HALFPI / lp.lam - lp.lam / M_HALFPI;
        c = lp.phi / M_HALFPI;
        d = (1 - c * c) / ((sp = sin(lp.phi)) - c);
        r2 = tb / d;
        r2 *= r2;
        m = (tb * sp / d - 0.5 * tb) / (1. + r2);
        n = (sp / r2 + 0.5 * d) / (1. + 1. / r2);
        xy.x = cos(lp.phi);
        xy.x = sqrt(m * m + xy.x * xy.x / (1. + r2));
        xy.x = M_HALFPI * (m + (lp.lam < 0. ? -xy.x : xy.x));
        xy.y = sqrt(n * n - (sp * sp / r2 + d * sp - 1.) / (1. + 1. / r2));
        xy.y = M_HALFPI * (n + (lp.phi < 0. ? xy.y : -xy.y));
    }
    return (xy);
}

PJ *PJ_PROJECTION(nicol) {
    P->es = 0.;
    P->fwd = nicol_s_forward;

    return P;
}
