

#include <errno.h>
#include <float.h>
#include <math.h>

#include "geodesic.h"
#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(gnom, "Gnomonic") "\n\tAzi, Sph";

#define EPS10 1.e-10

namespace pj_gnom_ns {
enum Mode { N_POLE = 0, S_POLE = 1, EQUIT = 2, OBLIQ = 3 };
}

namespace { // anonymous namespace
struct pj_gnom_data {
    double sinph0;
    double cosph0;
    enum pj_gnom_ns::Mode mode;
    struct geod_geodesic g;
};
} // anonymous namespace

static PJ_XY gnom_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_gnom_data *Q = static_cast<struct pj_gnom_data *>(P->opaque);
    double coslam, cosphi, sinphi;

    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    coslam = cos(lp.lam);

    switch (Q->mode) {
    case pj_gnom_ns::EQUIT:
        xy.y = cosphi * coslam;
        break;
    case pj_gnom_ns::OBLIQ:
        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi * coslam;
        break;
    case pj_gnom_ns::S_POLE:
        xy.y = -sinphi;
        break;
    case pj_gnom_ns::N_POLE:
        xy.y = sinphi;
        break;
    }

    if (xy.y <= EPS10) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }

    xy.x = (xy.y = 1. / xy.y) * cosphi * sin(lp.lam);
    switch (Q->mode) {
    case pj_gnom_ns::EQUIT:
        xy.y *= sinphi;
        break;
    case pj_gnom_ns::OBLIQ:
        xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi * coslam;
        break;
    case pj_gnom_ns::N_POLE:
        coslam = -coslam;
        PROJ_FALLTHROUGH;
    case pj_gnom_ns::S_POLE:
        xy.y *= cosphi * coslam;
        break;
    }
    return xy;
}

static PJ_LP gnom_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_gnom_data *Q = static_cast<struct pj_gnom_data *>(P->opaque);
    double rh, cosz, sinz;

    rh = hypot(xy.x, xy.y);
    sinz = sin(lp.phi = atan(rh));
    cosz = sqrt(1. - sinz * sinz);

    if (fabs(rh) <= EPS10) {
        lp.phi = P->phi0;
        lp.lam = 0.;
    } else {
        switch (Q->mode) {
        case pj_gnom_ns::OBLIQ:
            lp.phi = cosz * Q->sinph0 + xy.y * sinz * Q->cosph0 / rh;
            if (fabs(lp.phi) >= 1.)
                lp.phi = lp.phi > 0. ? M_HALFPI : -M_HALFPI;
            else
                lp.phi = asin(lp.phi);
            xy.y = (cosz - Q->sinph0 * sin(lp.phi)) * rh;
            xy.x *= sinz * Q->cosph0;
            break;
        case pj_gnom_ns::EQUIT:
            lp.phi = xy.y * sinz / rh;
            if (fabs(lp.phi) >= 1.)
                lp.phi = lp.phi > 0. ? M_HALFPI : -M_HALFPI;
            else
                lp.phi = asin(lp.phi);
            xy.y = cosz * rh;
            xy.x *= sinz;
            break;
        case pj_gnom_ns::S_POLE:
            lp.phi -= M_HALFPI;
            break;
        case pj_gnom_ns::N_POLE:
            lp.phi = M_HALFPI - lp.phi;
            xy.y = -xy.y;
            break;
        }
        lp.lam = atan2(xy.x, xy.y);
    }
    return lp;
}

static PJ_XY gnom_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_gnom_data *Q = static_cast<struct pj_gnom_data *>(P->opaque);

    double lat0 = P->phi0 / DEG_TO_RAD, lon0 = 0, lat1 = lp.phi / DEG_TO_RAD,
           lon1 = lp.lam / DEG_TO_RAD, azi0, m, M;

    geod_geninverse(&Q->g, lat0, lon0, lat1, lon1, nullptr, &azi0, nullptr, &m,
                    &M, nullptr, nullptr);
    if (M <= 0) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        xy.x = xy.y = HUGE_VAL;
    } else {
        double rho = m / M;
        azi0 *= DEG_TO_RAD;
        xy.x = rho * sin(azi0);
        xy.y = rho * cos(azi0);
    }
    return xy;
}

static PJ_LP gnom_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    constexpr int numit_ = 10;
    static const double eps_ = 0.01 * sqrt(DBL_EPSILON);

    PJ_LP lp = {0.0, 0.0};
    struct pj_gnom_data *Q = static_cast<struct pj_gnom_data *>(P->opaque);

    double lat0 = P->phi0 / DEG_TO_RAD, lon0 = 0,
           azi0 = atan2(xy.x, xy.y) / DEG_TO_RAD, // Clockwise from north
        rho = hypot(xy.x, xy.y), s = atan(rho);
    bool little = rho <= 1;
    if (!little)
        rho = 1 / rho;
    struct geod_geodesicline l;
    geod_lineinit(&l, &Q->g, lat0, lon0, azi0,
                  GEOD_LATITUDE | GEOD_LONGITUDE | GEOD_DISTANCE_IN |
                      GEOD_REDUCEDLENGTH | GEOD_GEODESICSCALE);

    int count = numit_, trip = 0;
    double lat1 = 0, lon1 = 0;
    while (count--) {
        double m, M;
        geod_genposition(&l, 0, s, &lat1, &lon1, nullptr, &s, &m, &M, nullptr,
                         nullptr);
        if (trip)
            break;
        // If little, solve rho(s) = rho with drho(s)/ds = 1/M^2
        // else solve 1/rho(s) = 1/rho with d(1/rho(s))/ds = -1/m^2
        double ds = little ? (m - rho * M) * M : (rho * m - M) * m;
        s -= ds;
        // Reversed test to allow escape with NaNs
        if (!(fabs(ds) >= eps_))
            ++trip;
    }
    if (trip) {
        lp.phi = lat1 * DEG_TO_RAD;
        lp.lam = lon1 * DEG_TO_RAD;
    } else {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        lp.phi = lp.lam = HUGE_VAL;
    }
    return lp;
}

PJ *PJ_PROJECTION(gnom) {
    struct pj_gnom_data *Q = static_cast<struct pj_gnom_data *>(
        calloc(1, sizeof(struct pj_gnom_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    if (P->es == 0) {
        if (fabs(fabs(P->phi0) - M_HALFPI) < EPS10) {
            Q->mode = P->phi0 < 0. ? pj_gnom_ns::S_POLE : pj_gnom_ns::N_POLE;
        } else if (fabs(P->phi0) < EPS10) {
            Q->mode = pj_gnom_ns::EQUIT;
        } else {
            Q->mode = pj_gnom_ns::OBLIQ;
            Q->sinph0 = sin(P->phi0);
            Q->cosph0 = cos(P->phi0);
        }

        P->inv = gnom_s_inverse;
        P->fwd = gnom_s_forward;
    } else {
        geod_init(&Q->g, 1, P->f);

        P->inv = gnom_e_inverse;
        P->fwd = gnom_e_forward;
    }
    P->es = 0.;

    return P;
}
