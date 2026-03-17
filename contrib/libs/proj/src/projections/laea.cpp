#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(laea, "Lambert Azimuthal Equal Area") "\n\tAzi, Sph&Ell";

namespace pj_laea_ns {
enum Mode { N_POLE = 0, S_POLE = 1, EQUIT = 2, OBLIQ = 3 };
}

namespace { // anonymous namespace
struct pj_laea_data {
    double sinb1;
    double cosb1;
    double xmf;
    double ymf;
    double mmf;
    double qp;
    double dd;
    double rq;
    double *apa;
    enum pj_laea_ns::Mode mode;
};
} // anonymous namespace

#define EPS10 1.e-10

static PJ_XY laea_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_laea_data *Q = static_cast<struct pj_laea_data *>(P->opaque);
    double coslam, sinlam, sinphi, cosphi, q, sinb = 0.0, cosb = 0.0, b = 0.0;

    coslam = cos(lp.lam);
    sinlam = sin(lp.lam);
    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    const double xi = pj_authalic_lat(lp.phi, sinphi, cosphi, Q->apa, P, Q->qp);
    q = sin(xi) * Q->qp;

    if (Q->mode == pj_laea_ns::OBLIQ || Q->mode == pj_laea_ns::EQUIT) {
        sinb = sin(xi);
        cosb = cos(xi);
    }

    switch (Q->mode) {
    case pj_laea_ns::OBLIQ:
        b = 1. + Q->sinb1 * sinb + Q->cosb1 * cosb * coslam;
        break;
    case pj_laea_ns::EQUIT:
        b = 1. + cosb * coslam;
        break;
    case pj_laea_ns::N_POLE:
        b = M_HALFPI + lp.phi;
        q = Q->qp - q;
        break;
    case pj_laea_ns::S_POLE:
        b = lp.phi - M_HALFPI;
        q = Q->qp + q;
        break;
    }
    if (fabs(b) < EPS10) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }

    switch (Q->mode) {
    case pj_laea_ns::OBLIQ:
        b = sqrt(2. / b);
        xy.y = Q->ymf * b * (Q->cosb1 * sinb - Q->sinb1 * cosb * coslam);
        goto eqcon;
        break;
    case pj_laea_ns::EQUIT:
        b = sqrt(2. / (1. + cosb * coslam));
        xy.y = b * sinb * Q->ymf;
    eqcon:
        xy.x = Q->xmf * b * cosb * sinlam;
        break;
    case pj_laea_ns::N_POLE:
    case pj_laea_ns::S_POLE:
        if (q >= 1e-15) {
            b = sqrt(q);
            xy.x = b * sinlam;
            xy.y = coslam * (Q->mode == pj_laea_ns::S_POLE ? b : -b);
        } else
            xy.x = xy.y = 0.;
        break;
    }
    return xy;
}

static PJ_XY laea_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_laea_data *Q = static_cast<struct pj_laea_data *>(P->opaque);
    double coslam, cosphi, sinphi;

    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    coslam = cos(lp.lam);
    switch (Q->mode) {
    case pj_laea_ns::EQUIT:
        xy.y = 1. + cosphi * coslam;
        goto oblcon;
    case pj_laea_ns::OBLIQ:
        xy.y = 1. + Q->sinb1 * sinphi + Q->cosb1 * cosphi * coslam;
    oblcon:
        if (xy.y <= EPS10) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        xy.y = sqrt(2. / xy.y);
        xy.x = xy.y * cosphi * sin(lp.lam);
        xy.y *= Q->mode == pj_laea_ns::EQUIT
                    ? sinphi
                    : Q->cosb1 * sinphi - Q->sinb1 * cosphi * coslam;
        break;
    case pj_laea_ns::N_POLE:
        coslam = -coslam;
        PROJ_FALLTHROUGH;
    case pj_laea_ns::S_POLE:
        if (fabs(lp.phi + P->phi0) < EPS10) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        xy.y = M_FORTPI - lp.phi * .5;
        xy.y = 2. * (Q->mode == pj_laea_ns::S_POLE ? cos(xy.y) : sin(xy.y));
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
    }
    return xy;
}

static PJ_LP laea_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_laea_data *Q = static_cast<struct pj_laea_data *>(P->opaque);
    double cCe, sCe, q, rho, ab = 0.0;

    switch (Q->mode) {
    case pj_laea_ns::EQUIT:
    case pj_laea_ns::OBLIQ: {
        xy.x /= Q->dd;
        xy.y *= Q->dd;
        rho = hypot(xy.x, xy.y);
        if (rho < EPS10) {
            lp.lam = 0.;
            lp.phi = P->phi0;
            return lp;
        }
        const double asin_argument = .5 * rho / Q->rq;
        if (asin_argument > 1) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        }
        sCe = 2. * asin(asin_argument);
        cCe = cos(sCe);
        sCe = sin(sCe);
        xy.x *= sCe;
        if (Q->mode == pj_laea_ns::OBLIQ) {
            ab = cCe * Q->sinb1 + xy.y * sCe * Q->cosb1 / rho;
            xy.y = rho * Q->cosb1 * cCe - xy.y * Q->sinb1 * sCe;
        } else {
            ab = xy.y * sCe / rho;
            xy.y = rho * cCe;
        }
        break;
    }
    case pj_laea_ns::N_POLE:
        xy.y = -xy.y;
        PROJ_FALLTHROUGH;
    case pj_laea_ns::S_POLE:
        q = (xy.x * xy.x + xy.y * xy.y);
        if (q == 0.0) {
            lp.lam = 0.;
            lp.phi = P->phi0;
            return (lp);
        }
        ab = 1. - q / Q->qp;
        if (Q->mode == pj_laea_ns::S_POLE)
            ab = -ab;
        break;
    }
    lp.lam = atan2(xy.x, xy.y);
    lp.phi = pj_authalic_lat_inverse(asin(ab), Q->apa, P, Q->qp);
    return lp;
}

static PJ_LP laea_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_laea_data *Q = static_cast<struct pj_laea_data *>(P->opaque);
    double cosz = 0.0, rh, sinz = 0.0;

    rh = hypot(xy.x, xy.y);
    if ((lp.phi = rh * .5) > 1.) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    lp.phi = 2. * asin(lp.phi);
    if (Q->mode == pj_laea_ns::OBLIQ || Q->mode == pj_laea_ns::EQUIT) {
        sinz = sin(lp.phi);
        cosz = cos(lp.phi);
    }
    switch (Q->mode) {
    case pj_laea_ns::EQUIT:
        lp.phi = fabs(rh) <= EPS10 ? 0. : asin(xy.y * sinz / rh);
        xy.x *= sinz;
        xy.y = cosz * rh;
        break;
    case pj_laea_ns::OBLIQ:
        lp.phi = fabs(rh) <= EPS10
                     ? P->phi0
                     : asin(cosz * Q->sinb1 + xy.y * sinz * Q->cosb1 / rh);
        xy.x *= sinz * Q->cosb1;
        xy.y = (cosz - sin(lp.phi) * Q->sinb1) * rh;
        break;
    case pj_laea_ns::N_POLE:
        xy.y = -xy.y;
        lp.phi = M_HALFPI - lp.phi;
        break;
    case pj_laea_ns::S_POLE:
        lp.phi -= M_HALFPI;
        break;
    }
    lp.lam = (xy.y == 0. &&
              (Q->mode == pj_laea_ns::EQUIT || Q->mode == pj_laea_ns::OBLIQ))
                 ? 0.
                 : atan2(xy.x, xy.y);
    return (lp);
}

static PJ *pj_laea_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_laea_data *>(P->opaque)->apa);

    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(laea) {
    double t;
    struct pj_laea_data *Q = static_cast<struct pj_laea_data *>(
        calloc(1, sizeof(struct pj_laea_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_laea_destructor;

    t = fabs(P->phi0);
    if (t > M_HALFPI + EPS10) {
        proj_log_error(P,
                       _("Invalid value for lat_0: |lat_0| should be <= 90°"));
        return pj_laea_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (fabs(t - M_HALFPI) < EPS10)
        Q->mode = P->phi0 < 0. ? pj_laea_ns::S_POLE : pj_laea_ns::N_POLE;
    else if (fabs(t) < EPS10)
        Q->mode = pj_laea_ns::EQUIT;
    else
        Q->mode = pj_laea_ns::OBLIQ;
    if (P->es != 0.0) {
        double sinphi, cosphi;

        P->e = sqrt(P->es);
        Q->qp = pj_authalic_lat_q(1.0, P);
        Q->mmf = .5 / (1. - P->es);
        Q->apa = pj_authalic_lat_compute_coeffs(P->n);
        if (nullptr == Q->apa)
            return pj_laea_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
        switch (Q->mode) {
        case pj_laea_ns::N_POLE:
        case pj_laea_ns::S_POLE:
            Q->dd = 1.;
            break;
        case pj_laea_ns::EQUIT:
            Q->dd = 1. / (Q->rq = sqrt(.5 * Q->qp));
            Q->xmf = 1.;
            Q->ymf = .5 * Q->qp;
            break;
        case pj_laea_ns::OBLIQ:
            Q->rq = sqrt(.5 * Q->qp);
            sinphi = sin(P->phi0);
            cosphi = cos(P->phi0);
            const double b1 =
                pj_authalic_lat(P->phi0, sinphi, cosphi, Q->apa, P, Q->qp);
            Q->sinb1 = sin(b1);
            Q->cosb1 = cos(b1);
            Q->dd = cos(P->phi0) /
                    (sqrt(1. - P->es * sinphi * sinphi) * Q->rq * Q->cosb1);
            Q->ymf = (Q->xmf = Q->rq) / Q->dd;
            Q->xmf *= Q->dd;
            break;
        }
        P->inv = laea_e_inverse;
        P->fwd = laea_e_forward;
    } else {
        if (Q->mode == pj_laea_ns::OBLIQ) {
            Q->sinb1 = sin(P->phi0);
            Q->cosb1 = cos(P->phi0);
        }
        P->inv = laea_s_inverse;
        P->fwd = laea_s_forward;
    }

    return P;
}
