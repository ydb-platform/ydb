
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(stere, "Stereographic") "\n\tAzi, Sph&Ell\n\tlat_ts=";
PROJ_HEAD(ups, "Universal Polar Stereographic") "\n\tAzi, Ell\n\tsouth";

namespace { // anonymous namespace
enum Mode { S_POLE = 0, N_POLE = 1, OBLIQ = 2, EQUIT = 3 };
} // anonymous namespace

namespace { // anonymous namespace
struct pj_stere {
    double phits;
    double sinX1;
    double cosX1;
    double akm1;
    enum Mode mode;
};
} // anonymous namespace

#define sinph0 static_cast<struct pj_stere *>(P->opaque)->sinX1
#define cosph0 static_cast<struct pj_stere *>(P->opaque)->cosX1
#define EPS10 1.e-10
#define TOL 1.e-8
#define NITER 8
#define CONV 1.e-10

static double ssfn_(double phit, double sinphi, double eccen) {
    sinphi *= eccen;
    return (tan(.5 * (M_HALFPI + phit)) *
            pow((1. - sinphi) / (1. + sinphi), .5 * eccen));
}

static PJ_XY stere_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_stere *Q = static_cast<struct pj_stere *>(P->opaque);
    double coslam, sinlam, sinX = 0.0, cosX = 0.0, A = 0.0, sinphi;

    coslam = cos(lp.lam);
    sinlam = sin(lp.lam);
    sinphi = sin(lp.phi);
    if (Q->mode == OBLIQ || Q->mode == EQUIT) {
        const double X = 2. * atan(ssfn_(lp.phi, sinphi, P->e)) - M_HALFPI;
        sinX = sin(X);
        cosX = cos(X);
    }

    switch (Q->mode) {
    case OBLIQ: {
        const double denom =
            Q->cosX1 * (1. + Q->sinX1 * sinX + Q->cosX1 * cosX * coslam);
        if (denom == 0) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return proj_coord_error().xy;
        }
        A = Q->akm1 / denom;
        xy.y = A * (Q->cosX1 * sinX - Q->sinX1 * cosX * coslam);
        xy.x = A * cosX;
        break;
    }

    case EQUIT:
        /* avoid zero division */
        if (1. + cosX * coslam == 0.0) {
            xy.y = HUGE_VAL;
        } else {
            A = Q->akm1 / (1. + cosX * coslam);
            xy.y = A * sinX;
        }
        xy.x = A * cosX;
        break;

    case S_POLE:
        lp.phi = -lp.phi;
        coslam = -coslam;
        sinphi = -sinphi;
        PROJ_FALLTHROUGH;
    case N_POLE:
        if (fabs(lp.phi - M_HALFPI) < 1e-15)
            xy.x = 0;
        else
            xy.x = Q->akm1 * pj_tsfn(lp.phi, sinphi, P->e);
        xy.y = -xy.x * coslam;
        break;
    }

    xy.x = xy.x * sinlam;
    return xy;
}

static PJ_XY stere_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_stere *Q = static_cast<struct pj_stere *>(P->opaque);
    double sinphi, cosphi, coslam, sinlam;

    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    coslam = cos(lp.lam);
    sinlam = sin(lp.lam);

    switch (Q->mode) {
    case EQUIT:
        xy.y = 1. + cosphi * coslam;
        goto oblcon;
    case OBLIQ:
        xy.y = 1. + sinph0 * sinphi + cosph0 * cosphi * coslam;
    oblcon:
        if (xy.y <= EPS10) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        xy.y = Q->akm1 / xy.y;
        xy.x = xy.y * cosphi * sinlam;
        xy.y *= (Q->mode == EQUIT) ? sinphi
                                   : cosph0 * sinphi - sinph0 * cosphi * coslam;
        break;
    case N_POLE:
        coslam = -coslam;
        lp.phi = -lp.phi;
        PROJ_FALLTHROUGH;
    case S_POLE:
        if (fabs(lp.phi - M_HALFPI) < TOL) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        xy.y = Q->akm1 * tan(M_FORTPI + .5 * lp.phi);
        xy.x = sinlam * xy.y;
        xy.y *= coslam;
        break;
    }
    return xy;
}

static PJ_LP stere_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_stere *Q = static_cast<struct pj_stere *>(P->opaque);
    double cosphi, sinphi, tp = 0.0, phi_l = 0.0, rho, halfe = 0.0,
                           halfpi = 0.0;

    rho = hypot(xy.x, xy.y);

    switch (Q->mode) {
    case OBLIQ:
    case EQUIT:
        tp = 2. * atan2(rho * Q->cosX1, Q->akm1);
        cosphi = cos(tp);
        sinphi = sin(tp);
        if (rho == 0.0)
            phi_l = asin(cosphi * Q->sinX1);
        else
            phi_l = asin(cosphi * Q->sinX1 + (xy.y * sinphi * Q->cosX1 / rho));

        tp = tan(.5 * (M_HALFPI + phi_l));
        xy.x *= sinphi;
        xy.y = rho * Q->cosX1 * cosphi - xy.y * Q->sinX1 * sinphi;
        halfpi = M_HALFPI;
        halfe = .5 * P->e;
        break;
    case N_POLE:
        xy.y = -xy.y;
        PROJ_FALLTHROUGH;
    case S_POLE:
        tp = -rho / Q->akm1;
        phi_l = M_HALFPI - 2. * atan(tp);
        halfpi = -M_HALFPI;
        halfe = -.5 * P->e;
        break;
    }

    for (int i = NITER; i > 0; --i) {
        sinphi = P->e * sin(phi_l);
        lp.phi =
            2. * atan(tp * pow((1. + sinphi) / (1. - sinphi), halfe)) - halfpi;
        if (fabs(phi_l - lp.phi) < CONV) {
            if (Q->mode == S_POLE)
                lp.phi = -lp.phi;
            lp.lam = (xy.x == 0. && xy.y == 0.) ? 0. : atan2(xy.x, xy.y);
            return lp;
        }
        phi_l = lp.phi;
    }

    proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    return lp;
}

static PJ_LP stere_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_stere *Q = static_cast<struct pj_stere *>(P->opaque);
    double c, sinc, cosc;

    const double rh = hypot(xy.x, xy.y);
    c = 2. * atan(rh / Q->akm1);
    sinc = sin(c);
    cosc = cos(c);
    lp.lam = 0.;

    switch (Q->mode) {
    case EQUIT:
        if (fabs(rh) <= EPS10)
            lp.phi = 0.;
        else
            lp.phi = asin(xy.y * sinc / rh);
        if (cosc != 0. || xy.x != 0.)
            lp.lam = atan2(xy.x * sinc, cosc * rh);
        break;
    case OBLIQ:
        if (fabs(rh) <= EPS10)
            lp.phi = P->phi0;
        else
            lp.phi = asin(cosc * sinph0 + xy.y * sinc * cosph0 / rh);
        c = cosc - sinph0 * sin(lp.phi);
        if (c != 0. || xy.x != 0.)
            lp.lam = atan2(xy.x * sinc * cosph0, c * rh);
        break;
    case N_POLE:
        xy.y = -xy.y;
        PROJ_FALLTHROUGH;
    case S_POLE:
        if (fabs(rh) <= EPS10)
            lp.phi = P->phi0;
        else
            lp.phi = asin(Q->mode == S_POLE ? -cosc : cosc);
        lp.lam = (xy.x == 0. && xy.y == 0.) ? 0. : atan2(xy.x, xy.y);
        break;
    }
    return lp;
}

static PJ *stere_setup(PJ *P) { /* general initialization */
    double t;
    struct pj_stere *Q = static_cast<struct pj_stere *>(P->opaque);

    if (fabs((t = fabs(P->phi0)) - M_HALFPI) < EPS10)
        Q->mode = P->phi0 < 0. ? S_POLE : N_POLE;
    else
        Q->mode = t > EPS10 ? OBLIQ : EQUIT;
    Q->phits = fabs(Q->phits);

    if (P->es != 0.0) {
        double X;

        switch (Q->mode) {
        case N_POLE:
        case S_POLE:
            if (fabs(Q->phits - M_HALFPI) < EPS10)
                Q->akm1 =
                    2. * P->k0 /
                    sqrt(pow(1 + P->e, 1 + P->e) * pow(1 - P->e, 1 - P->e));
            else {
                t = sin(Q->phits);
                Q->akm1 = cos(Q->phits) / pj_tsfn(Q->phits, t, P->e);
                t *= P->e;
                Q->akm1 /= sqrt(1. - t * t);
            }
            break;
        case EQUIT:
        case OBLIQ:
            t = sin(P->phi0);
            X = 2. * atan(ssfn_(P->phi0, t, P->e)) - M_HALFPI;
            t *= P->e;
            Q->akm1 = 2. * P->k0 * cos(P->phi0) / sqrt(1. - t * t);
            Q->sinX1 = sin(X);
            Q->cosX1 = cos(X);
            break;
        }
        P->inv = stere_e_inverse;
        P->fwd = stere_e_forward;
    } else {
        switch (Q->mode) {
        case OBLIQ:
            sinph0 = sin(P->phi0);
            cosph0 = cos(P->phi0);
            PROJ_FALLTHROUGH;
        case EQUIT:
            Q->akm1 = 2. * P->k0;
            break;
        case S_POLE:
        case N_POLE:
            Q->akm1 = fabs(Q->phits - M_HALFPI) >= EPS10
                          ? cos(Q->phits) / tan(M_FORTPI - .5 * Q->phits)
                          : 2. * P->k0;
            break;
        }

        P->inv = stere_s_inverse;
        P->fwd = stere_s_forward;
    }
    return P;
}

PJ *PJ_PROJECTION(stere) {
    struct pj_stere *Q =
        static_cast<struct pj_stere *>(calloc(1, sizeof(struct pj_stere)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->phits = pj_param(P->ctx, P->params, "tlat_ts").i
                   ? pj_param(P->ctx, P->params, "rlat_ts").f
                   : M_HALFPI;

    return stere_setup(P);
}

PJ *PJ_PROJECTION(ups) {
    struct pj_stere *Q =
        static_cast<struct pj_stere *>(calloc(1, sizeof(struct pj_stere)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    /* International Ellipsoid */
    P->phi0 = pj_param(P->ctx, P->params, "bsouth").i ? -M_HALFPI : M_HALFPI;
    if (P->es == 0.0) {
        proj_log_error(
            P,
            _("Invalid value for es: only ellipsoidal formulation supported"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    P->k0 = .994;
    P->x0 = 2000000.;
    P->y0 = 2000000.;
    Q->phits = M_HALFPI;
    P->lam0 = 0.;

    return stere_setup(P);
}

#undef sinph0
#undef cosph0
#undef EPS10
#undef TOL
#undef NITER
#undef CONV
