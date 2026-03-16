/*
  Created by Jacques Bertin in 1953, this projection was the go-to choice
  of the French cartographic school when they wished to represent phenomena
  on a global scale.

  Formula designed by Philippe Rivière, 2017.
  https://visionscarto.net/bertin-projection-1953

  Port to PROJ by Philippe Rivière, 21 September 2018
*/

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(bertin1953, "Bertin 1953")
"\n\tMisc Sph no inv.";

namespace { // anonymous namespace
struct pj_bertin1953 {
    double cos_delta_phi, sin_delta_phi, cos_delta_gamma, sin_delta_gamma,
        deltaLambda;
};
} // anonymous namespace

static PJ_XY bertin1953_s_forward(PJ_LP lp, PJ *P) {
    PJ_XY xy = {0.0, 0.0};
    struct pj_bertin1953 *Q = static_cast<struct pj_bertin1953 *>(P->opaque);

    double fu = 1.4, k = 12., w = 1.68, d;

    /* Rotate */
    double cosphi, x, y, z, z0;
    lp.lam += PJ_TORAD(-16.5);
    cosphi = cos(lp.phi);
    x = cos(lp.lam) * cosphi;
    y = sin(lp.lam) * cosphi;
    z = sin(lp.phi);
    z0 = z * Q->cos_delta_phi + x * Q->sin_delta_phi;
    lp.lam = atan2(y * Q->cos_delta_gamma - z0 * Q->sin_delta_gamma,
                   x * Q->cos_delta_phi - z * Q->sin_delta_phi);
    z0 = z0 * Q->cos_delta_gamma + y * Q->sin_delta_gamma;
    lp.phi = asin(z0);

    lp.lam = adjlon(lp.lam);

    /* Adjust pre-projection */
    if (lp.lam + lp.phi < -fu) {
        d = (lp.lam - lp.phi + 1.6) * (lp.lam + lp.phi + fu) / 8.;
        lp.lam += d;
        lp.phi -= 0.8 * d * sin(lp.phi + M_PI / 2.);
    }

    /* Project with Hammer (1.68,2) */
    cosphi = cos(lp.phi);
    d = sqrt(2. / (1. + cosphi * cos(lp.lam / 2.)));
    xy.x = w * d * cosphi * sin(lp.lam / 2.);
    xy.y = d * sin(lp.phi);

    /* Adjust post-projection */
    d = (1. - cos(lp.lam * lp.phi)) / k;
    if (xy.y < 0.) {
        xy.x *= 1. + d;
    }
    if (xy.y > 0.) {
        xy.y *= 1. + d / 1.5 * xy.x * xy.x;
    }

    return xy;
}

PJ *PJ_PROJECTION(bertin1953) {
    struct pj_bertin1953 *Q = static_cast<struct pj_bertin1953 *>(
        calloc(1, sizeof(struct pj_bertin1953)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    P->lam0 = 0;
    P->phi0 = PJ_TORAD(-42.);

    Q->cos_delta_phi = cos(P->phi0);
    Q->sin_delta_phi = sin(P->phi0);
    Q->cos_delta_gamma = 1.;
    Q->sin_delta_gamma = 0.;

    P->es = 0.;
    P->fwd = bertin1953_s_forward;

    return P;
}
