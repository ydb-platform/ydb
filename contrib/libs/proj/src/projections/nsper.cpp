
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

/* Note: EPSG Guidance 7-2 describes a Vertical Perspective method (EPSG::9838),
 * that extends 'nsper' with ellipsoidal development, and a ellipsoidal height
 * of topocentric origin for the projection plan.
 */

namespace pj_nsper_ns {
enum Mode { N_POLE = 0, S_POLE = 1, EQUIT = 2, OBLIQ = 3 };
}

namespace { // anonymous namespace
struct pj_nsper_data {
    double height;
    double sinph0;
    double cosph0;
    double p;
    double rp;
    double pn1;
    double pfact;
    double h;
    double cg;
    double sg;
    double sw;
    double cw;
    enum pj_nsper_ns::Mode mode;
    int tilt;
};
} // anonymous namespace

PROJ_HEAD(nsper, "Near-sided perspective") "\n\tAzi, Sph\n\th=";
PROJ_HEAD(tpers, "Tilted perspective") "\n\tAzi, Sph\n\ttilt= azi= h=";

#define EPS10 1.e-10

static PJ_XY nsper_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_nsper_data *Q = static_cast<struct pj_nsper_data *>(P->opaque);
    double coslam, cosphi, sinphi;

    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    coslam = cos(lp.lam);
    switch (Q->mode) {
    case pj_nsper_ns::OBLIQ:
        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi * coslam;
        break;
    case pj_nsper_ns::EQUIT:
        xy.y = cosphi * coslam;
        break;
    case pj_nsper_ns::S_POLE:
        xy.y = -sinphi;
        break;
    case pj_nsper_ns::N_POLE:
        xy.y = sinphi;
        break;
    }
    if (xy.y < Q->rp) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    xy.y = Q->pn1 / (Q->p - xy.y);
    xy.x = xy.y * cosphi * sin(lp.lam);
    switch (Q->mode) {
    case pj_nsper_ns::OBLIQ:
        xy.y *= (Q->cosph0 * sinphi - Q->sinph0 * cosphi * coslam);
        break;
    case pj_nsper_ns::EQUIT:
        xy.y *= sinphi;
        break;
    case pj_nsper_ns::N_POLE:
        coslam = -coslam;
        PROJ_FALLTHROUGH;
    case pj_nsper_ns::S_POLE:
        xy.y *= cosphi * coslam;
        break;
    }
    if (Q->tilt) {
        double yt, ba;

        yt = xy.y * Q->cg + xy.x * Q->sg;
        ba = 1. / (yt * Q->sw * Q->h + Q->cw);
        xy.x = (xy.x * Q->cg - xy.y * Q->sg) * Q->cw * ba;
        xy.y = yt * ba;
    }
    return xy;
}

static PJ_LP nsper_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_nsper_data *Q = static_cast<struct pj_nsper_data *>(P->opaque);
    double rh;

    if (Q->tilt) {
        double bm, bq, yt;

        yt = 1. / (Q->pn1 - xy.y * Q->sw);
        bm = Q->pn1 * xy.x * yt;
        bq = Q->pn1 * xy.y * Q->cw * yt;
        xy.x = bm * Q->cg + bq * Q->sg;
        xy.y = bq * Q->cg - bm * Q->sg;
    }
    rh = hypot(xy.x, xy.y);
    if (fabs(rh) <= EPS10) {
        lp.lam = 0.;
        lp.phi = P->phi0;
    } else {
        double cosz, sinz;
        sinz = 1. - rh * rh * Q->pfact;
        if (sinz < 0.) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        }
        sinz = (Q->p - sqrt(sinz)) / (Q->pn1 / rh + rh / Q->pn1);
        cosz = sqrt(1. - sinz * sinz);
        switch (Q->mode) {
        case pj_nsper_ns::OBLIQ:
            lp.phi = asin(cosz * Q->sinph0 + xy.y * sinz * Q->cosph0 / rh);
            xy.y = (cosz - Q->sinph0 * sin(lp.phi)) * rh;
            xy.x *= sinz * Q->cosph0;
            break;
        case pj_nsper_ns::EQUIT:
            lp.phi = asin(xy.y * sinz / rh);
            xy.y = cosz * rh;
            xy.x *= sinz;
            break;
        case pj_nsper_ns::N_POLE:
            lp.phi = asin(cosz);
            xy.y = -xy.y;
            break;
        case pj_nsper_ns::S_POLE:
            lp.phi = -asin(cosz);
            break;
        }
        lp.lam = atan2(xy.x, xy.y);
    }
    return lp;
}

static PJ *nsper_setup(PJ *P) {
    struct pj_nsper_data *Q = static_cast<struct pj_nsper_data *>(P->opaque);

    Q->height = pj_param(P->ctx, P->params, "dh").f;

    if (fabs(fabs(P->phi0) - M_HALFPI) < EPS10)
        Q->mode = P->phi0 < 0. ? pj_nsper_ns::S_POLE : pj_nsper_ns::N_POLE;
    else if (fabs(P->phi0) < EPS10)
        Q->mode = pj_nsper_ns::EQUIT;
    else {
        Q->mode = pj_nsper_ns::OBLIQ;
        Q->sinph0 = sin(P->phi0);
        Q->cosph0 = cos(P->phi0);
    }
    Q->pn1 = Q->height / P->a; /* normalize by radius */
    if (Q->pn1 <= 0 || Q->pn1 > 1e10) {
        proj_log_error(P, _("Invalid value for h"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    Q->p = 1. + Q->pn1;
    Q->rp = 1. / Q->p;
    Q->h = 1. / Q->pn1;
    Q->pfact = (Q->p + 1.) * Q->h;
    P->inv = nsper_s_inverse;
    P->fwd = nsper_s_forward;
    P->es = 0.;

    return P;
}

PJ *PJ_PROJECTION(nsper) {
    struct pj_nsper_data *Q = static_cast<struct pj_nsper_data *>(
        calloc(1, sizeof(struct pj_nsper_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->tilt = 0;

    return nsper_setup(P);
}

PJ *PJ_PROJECTION(tpers) {
    double omega, gamma;

    struct pj_nsper_data *Q = static_cast<struct pj_nsper_data *>(
        calloc(1, sizeof(struct pj_nsper_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    omega = pj_param(P->ctx, P->params, "rtilt").f;
    gamma = pj_param(P->ctx, P->params, "razi").f;
    Q->tilt = 1;
    Q->cg = cos(gamma);
    Q->sg = sin(gamma);
    Q->cw = cos(omega);
    Q->sw = sin(omega);

    return nsper_setup(P);
}

#undef EPS10
