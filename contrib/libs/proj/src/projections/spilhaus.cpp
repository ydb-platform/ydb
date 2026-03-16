/*
 * Implementation of the Spilhaus projections based on Adams World in a Square
 * II.
 *
 * Explained in https://github.com/OSGeo/PROJ/issues/1851
 *
 * Copyright (c) 2025 Javier Jimenez Shaw
 *
 * Related material
 * ----------------
 *
 * Map Projections - A Working Manual. 1987. John P. Snyder
 * Sections 3 and 5.
 * https://doi.org/10.3133/pp1395
 * Online at https://neacsu.net/docs/geodesy/snyder/2-general/
 */

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

C_NAMESPACE PJ *pj_adams_ws2(PJ *);

PROJ_HEAD(spilhaus, "Spilhaus") "\n\tSph&Ell";

namespace { // anonymous namespace
struct pj_spilhaus_data {
    double cosalpha;
    double sinalpha;
    double beta;
    double lambda_0;
    double conformal_distortion;
    double cosrot;
    double sinrot;
    PJ *adams_ws2;
};

} // anonymous namespace

static PJ_XY spilhaus_forward(PJ_LP lp, PJ *P) {
    PJ_XY xy = {0.0, 0.0};
    struct pj_spilhaus_data *Q =
        static_cast<struct pj_spilhaus_data *>(P->opaque);

    const double phi_c = pj_conformal_lat(lp.phi, P);
    const double cosphi_c = cos(phi_c);
    const double sinphi_c = sin(phi_c);

    const double coslam = cos(lp.lam - Q->lambda_0);
    const double sinlam = sin(lp.lam - Q->lambda_0);

    PJ_LP lpadams;
    // Snyder, A working manual, formula 5-7
    lpadams.phi =
        aasin(P->ctx, Q->sinalpha * sinphi_c - Q->cosalpha * cosphi_c * coslam);

    // Snyder, A working manual, formula 5-8b
    lpadams.lam =
        (Q->beta + atan2(cosphi_c * sinlam, (Q->sinalpha * cosphi_c * coslam +
                                             Q->cosalpha * sinphi_c)));

    while (lpadams.lam > M_PI)
        lpadams.lam -= M_PI * 2;
    while (lpadams.lam < -M_PI)
        lpadams.lam += M_PI * 2;

    PJ_XY xyadams = Q->adams_ws2->fwd(lpadams, Q->adams_ws2);
    const double factor = Q->conformal_distortion * P->k0;
    xy.x = -(xyadams.x * Q->cosrot + xyadams.y * Q->sinrot) * factor;
    xy.y = -(xyadams.x * -Q->sinrot + xyadams.y * Q->cosrot) * factor;

    return xy;
}

static PJ_LP spilhaus_inverse(PJ_XY xy, PJ *P) {
    PJ_LP lp = {0.0, 0.0};
    struct pj_spilhaus_data *Q =
        static_cast<struct pj_spilhaus_data *>(P->opaque);

    PJ_XY xyadams;
    const double factor = 1.0 / (Q->conformal_distortion * P->k0);
    xyadams.x = -(xy.x * Q->cosrot + xy.y * -Q->sinrot) * factor;
    xyadams.y = -(xy.x * Q->sinrot + xy.y * Q->cosrot) * factor;
    PJ_LP lpadams = Q->adams_ws2->inv(xyadams, Q->adams_ws2);

    const double cosphi_s = cos(lpadams.phi);
    const double sinphi_s = sin(lpadams.phi);
    const double coslam_s = cos(lpadams.lam - Q->beta);
    const double sinlam_s = sin(lpadams.lam - Q->beta);

    // conformal latitude
    lp.phi = aasin(P->ctx,
                   Q->sinalpha * sinphi_s + Q->cosalpha * cosphi_s * coslam_s);

    lp.lam = Q->lambda_0 +
             aatan2(cosphi_s * sinlam_s,
                    Q->sinalpha * cosphi_s * coslam_s - Q->cosalpha * sinphi_s);

    lp.phi = pj_conformal_lat_inverse(lp.phi, P);

    return lp;
}

static PJ *spilhaus_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;
    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);
    proj_destroy(static_cast<struct pj_spilhaus_data *>(P->opaque)->adams_ws2);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(spilhaus) {
    struct pj_spilhaus_data *Q = static_cast<struct pj_spilhaus_data *>(
        calloc(1, sizeof(struct pj_spilhaus_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = spilhaus_destructor;

    Q->adams_ws2 = pj_adams_ws2(nullptr);
    if (Q->adams_ws2 == nullptr)
        return spilhaus_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    Q->adams_ws2->ctx = P->ctx;
    Q->adams_ws2->e = 0;
    Q->adams_ws2 = pj_adams_ws2(Q->adams_ws2);
    if (Q->adams_ws2 == nullptr)
        return spilhaus_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    auto param_rad = [&P](const std::string &name, double def) {
        return pj_param(P->ctx, P->params, ("t" + name).c_str()).i
                   ? pj_param(P->ctx, P->params, ("r" + name).c_str()).f
                   : def * DEG_TO_RAD;
    };

    // Values from https://github.com/OSGeo/PROJ/issues/1851
    if (!pj_param(P->ctx, P->params, "tlon_0").i) {
        P->lam0 = 66.94970198 * DEG_TO_RAD;
    }
    if (!pj_param(P->ctx, P->params, "tlat_0").i) {
        P->phi0 = -49.56371678 * DEG_TO_RAD;
    }
    const double azimuth = param_rad("azi", 40.17823482);

    const double rotation = param_rad("rot", 45);
    Q->cosrot = cos(rotation);
    Q->sinrot = sin(rotation);

    const double conformal_lat_center = pj_conformal_lat(P->phi0, P);

    Q->sinalpha = -cos(conformal_lat_center) * cos(azimuth);
    Q->cosalpha = sqrt(1 - Q->sinalpha * Q->sinalpha);
    Q->lambda_0 = atan2(tan(azimuth), -sin(conformal_lat_center));
    Q->beta = M_PI + atan2(-sin(azimuth), -tan(conformal_lat_center));

    Q->conformal_distortion = cos(P->phi0) /
                              sqrt(1 - P->es * sin(P->phi0) * sin(P->phi0)) /
                              cos(conformal_lat_center);

    P->fwd = spilhaus_forward;
    P->inv = spilhaus_inverse;

    return P;
}
