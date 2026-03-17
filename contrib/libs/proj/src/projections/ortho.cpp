
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(ortho, "Orthographic") "\n\tAzi, Sph&Ell";

namespace pj_ortho_ns {
enum Mode { N_POLE = 0, S_POLE = 1, EQUIT = 2, OBLIQ = 3 };
}

namespace { // anonymous namespace
struct pj_ortho_data {
    double sinph0;
    double cosph0;
    double nu0;
    double y_shift;
    double y_scale;
    enum pj_ortho_ns::Mode mode;
    double sinalpha;
    double cosalpha;
};
} // anonymous namespace

#define EPS10 1.e-10

static PJ_XY forward_error(PJ *P, PJ_LP lp, PJ_XY xy) {
    proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    proj_log_trace(P,
                   "Coordinate (%.3f, %.3f) is on the unprojected hemisphere",
                   proj_todeg(lp.lam), proj_todeg(lp.phi));
    return xy;
}

static PJ_XY ortho_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy;
    struct pj_ortho_data *Q = static_cast<struct pj_ortho_data *>(P->opaque);
    double coslam, cosphi, sinphi;

    xy.x = HUGE_VAL;
    xy.y = HUGE_VAL;

    cosphi = cos(lp.phi);
    coslam = cos(lp.lam);
    switch (Q->mode) {
    case pj_ortho_ns::EQUIT:
        if (cosphi * coslam < -EPS10)
            return forward_error(P, lp, xy);
        xy.y = sin(lp.phi);
        break;
    case pj_ortho_ns::OBLIQ:
        sinphi = sin(lp.phi);

        // Is the point visible from the projection plane ?
        // From
        // https://lists.osgeo.org/pipermail/proj/2020-September/009831.html
        // this is the dot product of the normal of the ellipsoid at the center
        // of the projection and at the point considered for projection.
        // [cos(phi)*cos(lambda), cos(phi)*sin(lambda), sin(phi)]
        // Also from Snyder's Map Projection - A working manual, equation (5-3),
        // page 149
        if (Q->sinph0 * sinphi + Q->cosph0 * cosphi * coslam < -EPS10)
            return forward_error(P, lp, xy);
        xy.y = Q->cosph0 * sinphi - Q->sinph0 * cosphi * coslam;
        break;
    case pj_ortho_ns::N_POLE:
        coslam = -coslam;
        PROJ_FALLTHROUGH;
    case pj_ortho_ns::S_POLE:
        if (fabs(lp.phi - P->phi0) - EPS10 > M_HALFPI)
            return forward_error(P, lp, xy);
        xy.y = cosphi * coslam;
        break;
    }
    xy.x = cosphi * sin(lp.lam);

    const double xp = xy.x;
    const double yp = xy.y;
    xy.x = (xp * Q->cosalpha - yp * Q->sinalpha) * P->k0;
    xy.y = (xp * Q->sinalpha + yp * Q->cosalpha) * P->k0;
    return xy;
}

static PJ_LP ortho_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp;
    struct pj_ortho_data *Q = static_cast<struct pj_ortho_data *>(P->opaque);
    double sinc;

    lp.lam = HUGE_VAL;
    lp.phi = HUGE_VAL;

    const double xf = xy.x;
    const double yf = xy.y;
    xy.x = (Q->cosalpha * xf + Q->sinalpha * yf) / P->k0;
    xy.y = (-Q->sinalpha * xf + Q->cosalpha * yf) / P->k0;

    const double rh = hypot(xy.x, xy.y);
    sinc = rh;
    if (sinc > 1.) {
        if ((sinc - 1.) > EPS10) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        }
        sinc = 1.;
    }
    const double cosc = sqrt(1. - sinc * sinc); /* in this range OK */
    if (fabs(rh) <= EPS10) {
        lp.phi = P->phi0;
        lp.lam = 0.0;
    } else {
        switch (Q->mode) {
        case pj_ortho_ns::N_POLE:
            xy.y = -xy.y;
            lp.phi = acos(sinc);
            break;
        case pj_ortho_ns::S_POLE:
            lp.phi = -acos(sinc);
            break;
        case pj_ortho_ns::EQUIT:
            lp.phi = xy.y * sinc / rh;
            xy.x *= sinc;
            xy.y = cosc * rh;
            goto sinchk;
        case pj_ortho_ns::OBLIQ:
            lp.phi = cosc * Q->sinph0 + xy.y * sinc * Q->cosph0 / rh;
            xy.y = (cosc - Q->sinph0 * lp.phi) * rh;
            xy.x *= sinc * Q->cosph0;
        sinchk:
            if (fabs(lp.phi) >= 1.)
                lp.phi = lp.phi < 0. ? -M_HALFPI : M_HALFPI;
            else
                lp.phi = asin(lp.phi);
            break;
        }
        lp.lam = (xy.y == 0. && (Q->mode == pj_ortho_ns::OBLIQ ||
                                 Q->mode == pj_ortho_ns::EQUIT))
                     ? (xy.x == 0.  ? 0.
                        : xy.x < 0. ? -M_HALFPI
                                    : M_HALFPI)
                     : atan2(xy.x, xy.y);
    }
    return lp;
}

static PJ_XY ortho_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy;
    struct pj_ortho_data *Q = static_cast<struct pj_ortho_data *>(P->opaque);

    // From EPSG guidance note 7.2, March 2020, §3.3.5 Orthographic
    const double cosphi = cos(lp.phi);
    const double sinphi = sin(lp.phi);
    const double coslam = cos(lp.lam);
    const double sinlam = sin(lp.lam);

    // Is the point visible from the projection plane ?
    // Same condition as in spherical case
    if (Q->sinph0 * sinphi + Q->cosph0 * cosphi * coslam < -EPS10) {
        xy.x = HUGE_VAL;
        xy.y = HUGE_VAL;
        return forward_error(P, lp, xy);
    }

    const double nu = 1.0 / sqrt(1.0 - P->es * sinphi * sinphi);
    const double xp = nu * cosphi * sinlam;
    const double yp = nu * (sinphi * Q->cosph0 - cosphi * Q->sinph0 * coslam) +
                      P->es * (Q->nu0 * Q->sinph0 - nu * sinphi) * Q->cosph0;
    xy.x = (Q->cosalpha * xp - Q->sinalpha * yp) * P->k0;
    xy.y = (Q->sinalpha * xp + Q->cosalpha * yp) * P->k0;

    return xy;
}

static PJ_LP ortho_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp;
    struct pj_ortho_data *Q = static_cast<struct pj_ortho_data *>(P->opaque);

    const auto SQ = [](double x) { return x * x; };

    const double xf = xy.x;
    const double yf = xy.y;
    xy.x = (Q->cosalpha * xf + Q->sinalpha * yf) / P->k0;
    xy.y = (-Q->sinalpha * xf + Q->cosalpha * yf) / P->k0;

    if (Q->mode == pj_ortho_ns::N_POLE || Q->mode == pj_ortho_ns::S_POLE) {
        // Polar case. Forward case equations can be simplified as:
        // xy.x = nu * cosphi * sinlam
        // xy.y = nu * -cosphi * coslam * sign(phi0)
        // ==> lam = atan2(xy.x, -xy.y * sign(phi0))
        // ==> xy.x^2 + xy.y^2 = nu^2 * cosphi^2
        //                rh^2 = cosphi^2 / (1 - es * sinphi^2)
        // ==>  cosphi^2 = rh^2 * (1 - es) / (1 - es * rh^2)

        const double rh2 = SQ(xy.x) + SQ(xy.y);
        if (rh2 >= 1. - 1e-15) {
            if ((rh2 - 1.) > EPS10) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                lp.lam = HUGE_VAL;
                lp.phi = HUGE_VAL;
                return lp;
            }
            lp.phi = 0;
        } else {
            lp.phi = acos(sqrt(rh2 * P->one_es / (1 - P->es * rh2))) *
                     (Q->mode == pj_ortho_ns::N_POLE ? 1 : -1);
        }
        lp.lam = atan2(xy.x, xy.y * (Q->mode == pj_ortho_ns::N_POLE ? -1 : 1));
        return lp;
    }

    if (Q->mode == pj_ortho_ns::EQUIT) {
        // Equatorial case. Forward case equations can be simplified as:
        // xy.x = nu * cosphi * sinlam
        // xy.y  = nu * sinphi * (1 - P->es)
        // x^2 * (1 - es * sinphi^2) = (1 - sinphi^2) * sinlam^2
        // y^2 / ((1 - es)^2 + y^2 * es) = sinphi^2

        // Equation of the ellipse
        if (SQ(xy.x) + SQ(xy.y * (P->a / P->b)) > 1 + 1e-11) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            lp.lam = HUGE_VAL;
            lp.phi = HUGE_VAL;
            return lp;
        }

        const double sinphi2 =
            xy.y == 0 ? 0 : 1.0 / (SQ((1 - P->es) / xy.y) + P->es);
        if (sinphi2 > 1 - 1e-11) {
            lp.phi = M_PI_2 * (xy.y > 0 ? 1 : -1);
            lp.lam = 0;
            return lp;
        }
        lp.phi = asin(sqrt(sinphi2)) * (xy.y > 0 ? 1 : -1);
        const double sinlam =
            xy.x * sqrt((1 - P->es * sinphi2) / (1 - sinphi2));
        if (fabs(sinlam) - 1 > -1e-15)
            lp.lam = M_PI_2 * (xy.x > 0 ? 1 : -1);
        else
            lp.lam = asin(sinlam);
        return lp;
    }

    // Using Q->sinph0 * sinphi + Q->cosph0 * cosphi * coslam == 0 (visibity
    // condition of the forward case) in the forward equations, and a lot of
    // substitution games...
    PJ_XY xy_recentered;
    xy_recentered.x = xy.x;
    xy_recentered.y = (xy.y - Q->y_shift) / Q->y_scale;
    if (SQ(xy.x) + SQ(xy_recentered.y) > 1 + 1e-11) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        lp.lam = HUGE_VAL;
        lp.phi = HUGE_VAL;
        return lp;
    }

    // From EPSG guidance note 7.2, March 2020, §3.3.5 Orthographic

    // It suggests as initial guess:
    // lp.lam = 0;
    // lp.phi = P->phi0;
    // But for poles, this will not converge well. Better use:
    lp = ortho_s_inverse(xy_recentered, P);

    for (int i = 0; i < 20; i++) {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double one_minus_es_sinphi2 = 1.0 - P->es * sinphi * sinphi;
        const double nu = 1.0 / sqrt(one_minus_es_sinphi2);
        PJ_XY xy_new;
        xy_new.x = nu * cosphi * sinlam;
        xy_new.y = nu * (sinphi * Q->cosph0 - cosphi * Q->sinph0 * coslam) +
                   P->es * (Q->nu0 * Q->sinph0 - nu * sinphi) * Q->cosph0;
        const double rho = (1.0 - P->es) * nu / one_minus_es_sinphi2;
        const double J11 = -rho * sinphi * sinlam;
        const double J12 = nu * cosphi * coslam;
        const double J21 =
            rho * (cosphi * Q->cosph0 + sinphi * Q->sinph0 * coslam);
        const double J22 = nu * Q->sinph0 * cosphi * sinlam;
        const double D = J11 * J22 - J12 * J21;
        const double dx = xy.x - xy_new.x;
        const double dy = xy.y - xy_new.y;
        const double dphi = (J22 * dx - J12 * dy) / D;
        const double dlam = (-J21 * dx + J11 * dy) / D;
        lp.phi += dphi;
        if (lp.phi > M_PI_2) {
            lp.phi = M_PI_2 - (lp.phi - M_PI_2);
            lp.lam = adjlon(lp.lam + M_PI);
        } else if (lp.phi < -M_PI_2) {
            lp.phi = -M_PI_2 + (-M_PI_2 - lp.phi);
            lp.lam = adjlon(lp.lam + M_PI);
        }
        lp.lam += dlam;
        if (fabs(dphi) < 1e-12 && fabs(dlam) < 1e-12) {
            return lp;
        }
    }
    proj_context_errno_set(P->ctx,
                           PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    return lp;
}

PJ *PJ_PROJECTION(ortho) {
    struct pj_ortho_data *Q = static_cast<struct pj_ortho_data *>(
        calloc(1, sizeof(struct pj_ortho_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->sinph0 = sin(P->phi0);
    Q->cosph0 = cos(P->phi0);
    if (fabs(fabs(P->phi0) - M_HALFPI) <= EPS10)
        Q->mode = P->phi0 < 0. ? pj_ortho_ns::S_POLE : pj_ortho_ns::N_POLE;
    else if (fabs(P->phi0) > EPS10) {
        Q->mode = pj_ortho_ns::OBLIQ;
    } else
        Q->mode = pj_ortho_ns::EQUIT;
    if (P->es == 0) {
        P->inv = ortho_s_inverse;
        P->fwd = ortho_s_forward;
    } else {
        Q->nu0 = 1.0 / sqrt(1.0 - P->es * Q->sinph0 * Q->sinph0);
        Q->y_shift = P->es * Q->nu0 * Q->sinph0 * Q->cosph0;
        Q->y_scale = 1.0 / sqrt(1.0 - P->es * Q->cosph0 * Q->cosph0);
        P->inv = ortho_e_inverse;
        P->fwd = ortho_e_forward;
    }

    const double alpha = pj_param(P->ctx, P->params, "ralpha").f;
    Q->sinalpha = sin(alpha);
    Q->cosalpha = cos(alpha);

    return P;
}

#undef EPS10
