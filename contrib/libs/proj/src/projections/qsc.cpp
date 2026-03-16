/*
 * This implements the Quadrilateralized Spherical Cube (QSC) projection.
 *
 * Copyright (c) 2011, 2012  Martin Lambers <marlam@marlam.de>
 *
 * The QSC projection was introduced in:
 * [OL76]
 * E.M. O'Neill and R.E. Laubscher, "Extended Studies of a Quadrilateralized
 * Spherical Cube Earth Data Base", Naval Environmental Prediction Research
 * Facility Tech. Report NEPRF 3-76 (CSC), May 1976.
 *
 * The preceding shift from an ellipsoid to a sphere, which allows to apply
 * this projection to ellipsoids as used in the Ellipsoidal Cube Map model,
 * is described in
 * [LK12]
 * M. Lambers and A. Kolb, "Ellipsoidal Cube Maps for Accurate Rendering of
 * Planetary-Scale Terrain Data", Proc. Pacific Graphics (Short Papers), Sep.
 * 2012
 *
 * You have to choose one of the following projection centers,
 * corresponding to the centers of the six cube faces:
 * phi0 = 0.0, lam0 = 0.0       ("front" face)
 * phi0 = 0.0, lam0 = 90.0      ("right" face)
 * phi0 = 0.0, lam0 = 180.0     ("back" face)
 * phi0 = 0.0, lam0 = -90.0     ("left" face)
 * phi0 = 90.0                  ("top" face)
 * phi0 = -90.0                 ("bottom" face)
 * Other projection centers will not work!
 *
 * In the projection code below, each cube face is handled differently.
 * See the computation of the face parameter in the PROJECTION(qsc) function
 * and the handling of different face values (FACE_*) in the forward and
 * inverse projections.
 *
 * Furthermore, the projection is originally only defined for theta angles
 * between (-1/4 * PI) and (+1/4 * PI) on the current cube face. This area
 * of definition is named pj_qsc_ns::AREA_0 in the projection code below. The
 * other three areas of a cube face are handled by rotation of
 * pj_qsc_ns::AREA_0.
 */

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

/* The six cube faces. */
namespace pj_qsc_ns {
enum Face {
    FACE_FRONT = 0,
    FACE_RIGHT = 1,
    FACE_BACK = 2,
    FACE_LEFT = 3,
    FACE_TOP = 4,
    FACE_BOTTOM = 5
};
}

namespace { // anonymous namespace
struct pj_qsc_data {
    enum pj_qsc_ns::Face face;
    double a_squared;
    double b;
    double one_minus_f;
    double one_minus_f_squared;
};
} // anonymous namespace
PROJ_HEAD(qsc, "Quadrilateralized Spherical Cube") "\n\tAzi, Sph";

#define EPS10 1.e-10

/* The four areas on a cube face. AREA_0 is the area of definition,
 * the other three areas are counted counterclockwise. */
namespace pj_qsc_ns {
enum Area { AREA_0 = 0, AREA_1 = 1, AREA_2 = 2, AREA_3 = 3 };
}

/* Helper function for forward projection: compute the theta angle
 * and determine the area number. */
static double qsc_fwd_equat_face_theta(double phi, double y, double x,
                                       enum pj_qsc_ns::Area *area) {
    double theta;
    if (phi < EPS10) {
        *area = pj_qsc_ns::AREA_0;
        theta = 0.0;
    } else {
        theta = atan2(y, x);
        if (fabs(theta) <= M_FORTPI) {
            *area = pj_qsc_ns::AREA_0;
        } else if (theta > M_FORTPI && theta <= M_HALFPI + M_FORTPI) {
            *area = pj_qsc_ns::AREA_1;
            theta -= M_HALFPI;
        } else if (theta > M_HALFPI + M_FORTPI ||
                   theta <= -(M_HALFPI + M_FORTPI)) {
            *area = pj_qsc_ns::AREA_2;
            theta = (theta >= 0.0 ? theta - M_PI : theta + M_PI);
        } else {
            *area = pj_qsc_ns::AREA_3;
            theta += M_HALFPI;
        }
    }
    return theta;
}

/* Helper function: shift the longitude. */
static double qsc_shift_longitude_origin(double longitude, double offset) {
    double slon = longitude + offset;
    if (slon < -M_PI) {
        slon += M_TWOPI;
    } else if (slon > +M_PI) {
        slon -= M_TWOPI;
    }
    return slon;
}

static PJ_XY qsc_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_qsc_data *Q = static_cast<struct pj_qsc_data *>(P->opaque);
    double lat, longitude;
    double theta, phi;
    double t, mu; /* nu; */
    enum pj_qsc_ns::Area area;

    /* Convert the geodetic latitude to a geocentric latitude.
     * This corresponds to the shift from the ellipsoid to the sphere
     * described in [LK12]. */
    if (P->es != 0.0) {
        lat = atan(Q->one_minus_f_squared * tan(lp.phi));
    } else {
        lat = lp.phi;
    }

    /* Convert the input lat, longitude into theta, phi as used by QSC.
     * This depends on the cube face and the area on it.
     * For the top and bottom face, we can compute theta and phi
     * directly from phi, lam. For the other faces, we must use
     * unit sphere cartesian coordinates as an intermediate step. */
    longitude = lp.lam;
    if (Q->face == pj_qsc_ns::FACE_TOP) {
        phi = M_HALFPI - lat;
        if (longitude >= M_FORTPI && longitude <= M_HALFPI + M_FORTPI) {
            area = pj_qsc_ns::AREA_0;
            theta = longitude - M_HALFPI;
        } else if (longitude > M_HALFPI + M_FORTPI ||
                   longitude <= -(M_HALFPI + M_FORTPI)) {
            area = pj_qsc_ns::AREA_1;
            theta = (longitude > 0.0 ? longitude - M_PI : longitude + M_PI);
        } else if (longitude > -(M_HALFPI + M_FORTPI) &&
                   longitude <= -M_FORTPI) {
            area = pj_qsc_ns::AREA_2;
            theta = longitude + M_HALFPI;
        } else {
            area = pj_qsc_ns::AREA_3;
            theta = longitude;
        }
    } else if (Q->face == pj_qsc_ns::FACE_BOTTOM) {
        phi = M_HALFPI + lat;
        if (longitude >= M_FORTPI && longitude <= M_HALFPI + M_FORTPI) {
            area = pj_qsc_ns::AREA_0;
            theta = -longitude + M_HALFPI;
        } else if (longitude < M_FORTPI && longitude >= -M_FORTPI) {
            area = pj_qsc_ns::AREA_1;
            theta = -longitude;
        } else if (longitude < -M_FORTPI &&
                   longitude >= -(M_HALFPI + M_FORTPI)) {
            area = pj_qsc_ns::AREA_2;
            theta = -longitude - M_HALFPI;
        } else {
            area = pj_qsc_ns::AREA_3;
            theta = (longitude > 0.0 ? -longitude + M_PI : -longitude - M_PI);
        }
    } else {
        double q, r, s;
        double sinlat, coslat;
        double sinlon, coslon;

        if (Q->face == pj_qsc_ns::FACE_RIGHT) {
            longitude = qsc_shift_longitude_origin(longitude, +M_HALFPI);
        } else if (Q->face == pj_qsc_ns::FACE_BACK) {
            longitude = qsc_shift_longitude_origin(longitude, +M_PI);
        } else if (Q->face == pj_qsc_ns::FACE_LEFT) {
            longitude = qsc_shift_longitude_origin(longitude, -M_HALFPI);
        }
        sinlat = sin(lat);
        coslat = cos(lat);
        sinlon = sin(longitude);
        coslon = cos(longitude);
        q = coslat * coslon;
        r = coslat * sinlon;
        s = sinlat;

        if (Q->face == pj_qsc_ns::FACE_FRONT) {
            phi = acos(q);
            theta = qsc_fwd_equat_face_theta(phi, s, r, &area);
        } else if (Q->face == pj_qsc_ns::FACE_RIGHT) {
            phi = acos(r);
            theta = qsc_fwd_equat_face_theta(phi, s, -q, &area);
        } else if (Q->face == pj_qsc_ns::FACE_BACK) {
            phi = acos(-q);
            theta = qsc_fwd_equat_face_theta(phi, s, -r, &area);
        } else if (Q->face == pj_qsc_ns::FACE_LEFT) {
            phi = acos(-r);
            theta = qsc_fwd_equat_face_theta(phi, s, q, &area);
        } else {
            /* Impossible */
            phi = theta = 0.0;
            area = pj_qsc_ns::AREA_0;
        }
    }

    /* Compute mu and nu for the area of definition.
     * For mu, see Eq. (3-21) in [OL76], but note the typos:
     * compare with Eq. (3-14). For nu, see Eq. (3-38). */
    mu = atan((12.0 / M_PI) *
              (theta + acos(sin(theta) * cos(M_FORTPI)) - M_HALFPI));
    t = sqrt((1.0 - cos(phi)) / (cos(mu) * cos(mu)) /
             (1.0 - cos(atan(1.0 / cos(theta)))));
    /* nu = atan(t);        We don't really need nu, just t, see below. */

    /* Apply the result to the real area. */
    if (area == pj_qsc_ns::AREA_1) {
        mu += M_HALFPI;
    } else if (area == pj_qsc_ns::AREA_2) {
        mu += M_PI;
    } else if (area == pj_qsc_ns::AREA_3) {
        mu += M_PI_HALFPI;
    }

    /* Now compute x, y from mu and nu */
    /* t = tan(nu); */
    xy.x = t * cos(mu);
    xy.y = t * sin(mu);
    return xy;
}

static PJ_LP qsc_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_qsc_data *Q = static_cast<struct pj_qsc_data *>(P->opaque);
    double mu, nu, cosmu, tannu;
    double tantheta, theta, cosphi, phi;
    double t;
    int area;

    /* Convert the input x, y to the mu and nu angles as used by QSC.
     * This depends on the area of the cube face. */
    nu = atan(sqrt(xy.x * xy.x + xy.y * xy.y));
    mu = atan2(xy.y, xy.x);
    if (xy.x >= 0.0 && xy.x >= fabs(xy.y)) {
        area = pj_qsc_ns::AREA_0;
    } else if (xy.y >= 0.0 && xy.y >= fabs(xy.x)) {
        area = pj_qsc_ns::AREA_1;
        mu -= M_HALFPI;
    } else if (xy.x < 0.0 && -xy.x >= fabs(xy.y)) {
        area = pj_qsc_ns::AREA_2;
        mu = (mu < 0.0 ? mu + M_PI : mu - M_PI);
    } else {
        area = pj_qsc_ns::AREA_3;
        mu += M_HALFPI;
    }

    /* Compute phi and theta for the area of definition.
     * The inverse projection is not described in the original paper, but some
     * good hints can be found here (as of 2011-12-14):
     * http://fits.gsfc.nasa.gov/fitsbits/saf.93/saf.9302
     * (search for "Message-Id: <9302181759.AA25477 at fits.cv.nrao.edu>") */
    t = (M_PI / 12.0) * tan(mu);
    tantheta = sin(t) / (cos(t) - (1.0 / sqrt(2.0)));
    theta = atan(tantheta);
    cosmu = cos(mu);
    tannu = tan(nu);
    cosphi = 1.0 - cosmu * cosmu * tannu * tannu *
                       (1.0 - cos(atan(1.0 / cos(theta))));
    if (cosphi < -1.0) {
        cosphi = -1.0;
    } else if (cosphi > +1.0) {
        cosphi = +1.0;
    }

    /* Apply the result to the real area on the cube face.
     * For the top and bottom face, we can compute phi and lam directly.
     * For the other faces, we must use unit sphere cartesian coordinates
     * as an intermediate step. */
    if (Q->face == pj_qsc_ns::FACE_TOP) {
        phi = acos(cosphi);
        lp.phi = M_HALFPI - phi;
        if (area == pj_qsc_ns::AREA_0) {
            lp.lam = theta + M_HALFPI;
        } else if (area == pj_qsc_ns::AREA_1) {
            lp.lam = (theta < 0.0 ? theta + M_PI : theta - M_PI);
        } else if (area == pj_qsc_ns::AREA_2) {
            lp.lam = theta - M_HALFPI;
        } else /* area == pj_qsc_ns::AREA_3 */ {
            lp.lam = theta;
        }
    } else if (Q->face == pj_qsc_ns::FACE_BOTTOM) {
        phi = acos(cosphi);
        lp.phi = phi - M_HALFPI;
        if (area == pj_qsc_ns::AREA_0) {
            lp.lam = -theta + M_HALFPI;
        } else if (area == pj_qsc_ns::AREA_1) {
            lp.lam = -theta;
        } else if (area == pj_qsc_ns::AREA_2) {
            lp.lam = -theta - M_HALFPI;
        } else /* area == pj_qsc_ns::AREA_3 */ {
            lp.lam = (theta < 0.0 ? -theta - M_PI : -theta + M_PI);
        }
    } else {
        /* Compute phi and lam via cartesian unit sphere coordinates. */
        double q, r, s;
        q = cosphi;
        t = q * q;
        if (t >= 1.0) {
            s = 0.0;
        } else {
            s = sqrt(1.0 - t) * sin(theta);
        }
        t += s * s;
        if (t >= 1.0) {
            r = 0.0;
        } else {
            r = sqrt(1.0 - t);
        }
        /* Rotate q,r,s into the correct area. */
        if (area == pj_qsc_ns::AREA_1) {
            t = r;
            r = -s;
            s = t;
        } else if (area == pj_qsc_ns::AREA_2) {
            r = -r;
            s = -s;
        } else if (area == pj_qsc_ns::AREA_3) {
            t = r;
            r = s;
            s = -t;
        }
        /* Rotate q,r,s into the correct cube face. */
        if (Q->face == pj_qsc_ns::FACE_RIGHT) {
            t = q;
            q = -r;
            r = t;
        } else if (Q->face == pj_qsc_ns::FACE_BACK) {
            q = -q;
            r = -r;
        } else if (Q->face == pj_qsc_ns::FACE_LEFT) {
            t = q;
            q = r;
            r = -t;
        }
        /* Now compute phi and lam from the unit sphere coordinates. */
        lp.phi = acos(-s) - M_HALFPI;
        lp.lam = atan2(r, q);
        if (Q->face == pj_qsc_ns::FACE_RIGHT) {
            lp.lam = qsc_shift_longitude_origin(lp.lam, -M_HALFPI);
        } else if (Q->face == pj_qsc_ns::FACE_BACK) {
            lp.lam = qsc_shift_longitude_origin(lp.lam, -M_PI);
        } else if (Q->face == pj_qsc_ns::FACE_LEFT) {
            lp.lam = qsc_shift_longitude_origin(lp.lam, +M_HALFPI);
        }
    }

    /* Apply the shift from the sphere to the ellipsoid as described
     * in [LK12]. */
    if (P->es != 0.0) {
        int invert_sign;
        double tanphi, xa;
        invert_sign = (lp.phi < 0.0 ? 1 : 0);
        tanphi = tan(lp.phi);
        xa = Q->b / sqrt(tanphi * tanphi + Q->one_minus_f_squared);
        lp.phi = atan(sqrt(P->a * P->a - xa * xa) / (Q->one_minus_f * xa));
        if (invert_sign) {
            lp.phi = -lp.phi;
        }
    }
    return lp;
}

PJ *PJ_PROJECTION(qsc) {
    struct pj_qsc_data *Q = static_cast<struct pj_qsc_data *>(
        calloc(1, sizeof(struct pj_qsc_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    P->inv = qsc_e_inverse;
    P->fwd = qsc_e_forward;
    /* Determine the cube face from the center of projection. */
    if (P->phi0 >= M_HALFPI - M_FORTPI / 2.0) {
        Q->face = pj_qsc_ns::FACE_TOP;
    } else if (P->phi0 <= -(M_HALFPI - M_FORTPI / 2.0)) {
        Q->face = pj_qsc_ns::FACE_BOTTOM;
    } else if (fabs(P->lam0) <= M_FORTPI) {
        Q->face = pj_qsc_ns::FACE_FRONT;
    } else if (fabs(P->lam0) <= M_HALFPI + M_FORTPI) {
        Q->face =
            (P->lam0 > 0.0 ? pj_qsc_ns::FACE_RIGHT : pj_qsc_ns::FACE_LEFT);
    } else {
        Q->face = pj_qsc_ns::FACE_BACK;
    }
    /* Fill in useful values for the ellipsoid <-> sphere shift
     * described in [LK12]. */
    if (P->es != 0.0) {
        Q->a_squared = P->a * P->a;
        Q->b = P->a * sqrt(1.0 - P->es);
        Q->one_minus_f = 1.0 - (P->a - Q->b) / P->a;
        Q->one_minus_f_squared = Q->one_minus_f * Q->one_minus_f;
    }

    return P;
}

#undef EPS10
