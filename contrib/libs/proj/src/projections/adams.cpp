/*
 * Implementation of the Guyou, Pierce Quincuncial, Adams Hemisphere in a
 * Square, Adams World in a Square I & II projections.
 *
 * Based on original code from libproj4 written by Gerald Evenden. Adapted to
 * modern PROJ by Kristian Evers. Original code found in file src/proj_guyou.c,
 * see
 * https://github.com/rouault/libproj4/blob/master/libproject-1.01/src/proj_guyou.c
 * for reference.
 * Fix for Peirce Quincuncial projection to diamond or square by Toby C.
 * Wilkinson to correctly flip out southern hemisphere into the four triangles
 * of Peirce's quincunx. The fix inspired by a similar rotate and translate
 * solution applied by Jonathan Feinberg for cartopy, see
 * https://github.com/jonathf/cartopy/blob/8172cac7fc45cafc86573d408ce85b74258a9c28/lib/cartopy/peircequincuncial.py
 * Added original code for horizontal and vertical arrangement of hemispheres by
 * Toby C. Wilkinson to allow creation of lateral quincuncial projections, such
 * as Grieger's Triptychial, see, e.g.:
 * - Grieger, B. (2020). “Optimized global map projections for specific
 * applications: the triptychial projection and the Spilhaus projection”.
 * EGU2020-9885. https://doi.org/10.5194/egusphere-egu2020-9885
 *
 * Copyright (c) 2005, 2006, 2009 Gerald I. Evenden
 * Copyright (c) 2020 Kristian Evers
 * Copyright (c) 2021 Toby C Wilkinson
 *
 * Related material
 * ----------------
 *
 *  CONFORMAL PROJECTION OF THE SPHERE WITHIN A SQUARE, 1929, OSCAR S. ADAMS,
 *  U.S. COAST AND GEODETIC SURVEY, Special Publication No.153,
 *  ftp://ftp.library.noaa.gov/docs.lib/htdocs/rescue/cgs_specpubs/QB275U35no1531929.pdf
 *
 *  https://en.wikipedia.org/wiki/Guyou_hemisphere-in-a-square_projection
 *  https://en.wikipedia.org/wiki/Adams_hemisphere-in-a-square_projection
 *  https://en.wikipedia.org/wiki/Peirce_quincuncial_projection
 */

#include <errno.h>
#include <math.h>

#include <algorithm>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(guyou, "Guyou") "\n\tMisc Sph No inv";
PROJ_HEAD(peirce_q, "Peirce Quincuncial") "\n\tMisc Sph No inv";
PROJ_HEAD(adams_hemi, "Adams Hemisphere in a Square") "\n\tMisc Sph No inv";
PROJ_HEAD(adams_ws1, "Adams World in a Square I") "\n\tMisc Sph No inv";
PROJ_HEAD(adams_ws2, "Adams World in a Square II") "\n\tMisc Sph No inv";

namespace { // anonymous namespace

enum projection_type {
    GUYOU,
    PEIRCE_Q,
    ADAMS_HEMI,
    ADAMS_WS1,
    ADAMS_WS2,
};

enum peirce_shape {
    PEIRCE_Q_SQUARE,
    PEIRCE_Q_DIAMOND,
    PEIRCE_Q_NHEMISPHERE,
    PEIRCE_Q_SHEMISPHERE,
    PEIRCE_Q_HORIZONTAL,
    PEIRCE_Q_VERTICAL,
};

struct pj_adams_data {
    projection_type mode;
    peirce_shape pqshape;
    double scrollx = 0.0;
    double scrolly = 0.0;
};

} // anonymous namespace

#define TOL 1e-9
#define RSQRT2 0.7071067811865475244008443620

static double ell_int_5(double phi) {
    /* Procedure to compute elliptic integral of the first kind
     * where k^2=0.5.  Precision good to better than 1e-7
     * The approximation is performed with an even Chebyshev
     * series, thus the coefficients below are the even values
     * and where series evaluation  must be multiplied by the argument. */
    constexpr double C0 = 2.19174570831038;
    static const double C[] = {
        -8.58691003636495e-07, 2.02692115653689e-07, 3.12960480765314e-05,
        5.30394739921063e-05,  -0.0012804644680613,  -0.00575574836830288,
        0.0914203033408211,
    };

    double y = phi * M_2_PI;
    y = 2. * y * y - 1.;
    double y2 = 2. * y;
    double d1 = 0.0;
    double d2 = 0.0;
    for (double c : C) {
        double temp = d1;
        d1 = y2 * d1 - d2 + c;
        d2 = temp;
    }

    return phi * (y * d1 - d2 + 0.5 * C0);
}

static PJ_XY adams_forward(PJ_LP lp, PJ *P) {
    double a = 0., b = 0.;
    bool sm = false, sn = false;
    PJ_XY xy;
    const struct pj_adams_data *Q =
        static_cast<const struct pj_adams_data *>(P->opaque);

    switch (Q->mode) {
    case GUYOU:
        if ((fabs(lp.lam) - TOL) > M_PI_2) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return proj_coord_error().xy;
        }

        if (fabs(fabs(lp.phi) - M_PI_2) < TOL) {
            xy.x = 0;
            xy.y = lp.phi < 0 ? -1.85407 : 1.85407;
            return xy;
        } else {
            const double sl = sin(lp.lam);
            const double sp = sin(lp.phi);
            const double cp = cos(lp.phi);
            a = aacos(P->ctx, (cp * sl - sp) * RSQRT2);
            b = aacos(P->ctx, (cp * sl + sp) * RSQRT2);
            sm = lp.lam < 0.;
            sn = lp.phi < 0.;
        }
        break;
    case PEIRCE_Q: {
        /* lam0 - note that the original Peirce model used a central meridian of
         * around -70deg, but the default within proj is +lon0=0 */
        if (Q->pqshape == PEIRCE_Q_NHEMISPHERE) {
            if (lp.phi < -TOL) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return proj_coord_error().xy;
            }
        }
        if (Q->pqshape == PEIRCE_Q_SHEMISPHERE) {
            if (lp.phi > -TOL) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return proj_coord_error().xy;
            }
        }
        const double sl = sin(lp.lam);
        const double cl = cos(lp.lam);
        const double cp = cos(lp.phi);
        a = aacos(P->ctx, cp * (sl + cl) * RSQRT2);
        b = aacos(P->ctx, cp * (sl - cl) * RSQRT2);
        sm = sl < 0.;
        sn = cl > 0.;
    } break;
    case ADAMS_HEMI: {
        const double sp = sin(lp.phi);
        if ((fabs(lp.lam) - TOL) > M_PI_2) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return proj_coord_error().xy;
        }
        a = cos(lp.phi) * sin(lp.lam);
        sm = (sp + a) < 0.;
        sn = (sp - a) < 0.;
        a = aacos(P->ctx, a);
        b = M_PI_2 - lp.phi;
    } break;
    case ADAMS_WS1: {
        const double sp = tan(0.5 * lp.phi);
        b = cos(aasin(P->ctx, sp)) * sin(0.5 * lp.lam);
        a = aacos(P->ctx, (b - sp) * RSQRT2);
        b = aacos(P->ctx, (b + sp) * RSQRT2);
        sm = lp.lam < 0.;
        sn = lp.phi < 0.;
    } break;
    case ADAMS_WS2: {
        const double spp = tan(0.5 * lp.phi);
        a = cos(aasin(P->ctx, spp)) * sin(0.5 * lp.lam);
        sm = (spp + a) < 0.;
        sn = (spp - a) < 0.;
        b = aacos(P->ctx, spp);
        a = aacos(P->ctx, a);
    } break;
    }

    double m = aasin(P->ctx, sqrt((1. + std::min(0.0, cos(a + b)))));
    if (sm)
        m = -m;

    double n = aasin(P->ctx, sqrt(fabs(1. - std::max(0.0, cos(a - b)))));
    if (sn)
        n = -n;

    xy.x = ell_int_5(m);
    xy.y = ell_int_5(n);

    if (Q->mode == PEIRCE_Q) {
        /* Constant complete elliptic integral of the first kind with m=0.5,
         * calculated using
         * https://docs.scipy.org/doc/scipy/reference/generated/scipy.special.ellipk.html
         * . Used as basic as scaled shift distance */
        constexpr double shd = 1.8540746773013719 * 2;

        /* For square and diamond Quincuncial projections, spin out southern
         * hemisphere to triangular segments of quincunx (before rotation for
         * square)*/
        if (Q->pqshape == PEIRCE_Q_SQUARE || (Q->pqshape == PEIRCE_Q_DIAMOND)) {
            if (lp.phi < 0.) { /* fold out segments */
                if (lp.lam < (-0.75 * M_PI))
                    xy.y = shd -
                           xy.y; /* top left segment, shift up and reflect y */
                if ((lp.lam < (-0.25 * M_PI)) && (lp.lam >= (-0.75 * M_PI)))
                    xy.x = -shd -
                           xy.x; /* left segment, shift left and reflect x */
                if ((lp.lam < (0.25 * M_PI)) && (lp.lam >= (-0.25 * M_PI)))
                    xy.y = -shd -
                           xy.y; /* bottom segment, shift down and reflect y */
                if ((lp.lam < (0.75 * M_PI)) && (lp.lam >= (0.25 * M_PI)))
                    xy.x = shd -
                           xy.x; /* right segment, shift right and reflect x */
                if (lp.lam >= (0.75 * M_PI))
                    xy.y = shd -
                           xy.y; /* top right segment, shift up and reflect y */
            }
        }

        /* For square types rotate xy by 45 deg */
        if (Q->pqshape == PEIRCE_Q_SQUARE) {
            const double temp = xy.x;
            xy.x = RSQRT2 * (xy.x - xy.y);
            xy.y = RSQRT2 * (temp + xy.y);
        }

        /* For rectangle Quincuncial projs, spin out southern hemisphere to east
         * (horizontal) or north (vertical) after rotation */
        if (Q->pqshape == PEIRCE_Q_HORIZONTAL) {
            if (lp.phi < 0.) {
                xy.x = shd - xy.x; /* reflect x to east */
            }
            xy.x = xy.x - (shd / 2); /* shift everything so origin is in middle
                                        of two hemispheres */
        }
        if (Q->pqshape == PEIRCE_Q_VERTICAL) {
            if (lp.phi < 0.) {
                xy.y = shd - xy.y; /* reflect y to north */
            }
            xy.y = xy.y - (shd / 2); /* shift everything so origin is in middle
                                        of two hemispheres */
        }

        // if o_scrollx param present, scroll x
        if (!(Q->scrollx == 0.0) && (Q->pqshape == PEIRCE_Q_HORIZONTAL)) {
            double xscale = 2.0;
            double xthresh = shd / 2;
            xy.x = xy.x +
                   (Q->scrollx *
                    (xthresh * 2 * xscale)); /*shift relative to proj width*/
            if (xy.x >= (xthresh * xscale)) {
                xy.x = xy.x - (shd * xscale);
            } else if (xy.x < -(xthresh * xscale)) {
                xy.x = xy.x + (shd * xscale);
            }
        }

        // if o_scrolly param present, scroll y
        if (!(Q->scrolly == 0.0) && (Q->pqshape == PEIRCE_Q_VERTICAL)) {
            double yscale = 2.0;
            double ythresh = shd / 2;
            xy.y = xy.y +
                   (Q->scrolly *
                    (ythresh * 2 * yscale)); /*shift relative to proj height*/
            if (xy.y >= (ythresh * yscale)) {
                xy.y = xy.y - (shd * yscale);
            } else if (xy.y < -(ythresh * yscale)) {
                xy.y = xy.y + (shd * yscale);
            }
        }
    }

    if (Q->mode == ADAMS_HEMI || Q->mode == ADAMS_WS2) { /* rotate by 45deg. */
        const double temp = xy.x;
        xy.x = RSQRT2 * (xy.x - xy.y);
        xy.y = RSQRT2 * (temp + xy.y);
    }

    return xy;
}

static PJ_LP adams_inverse(PJ_XY xy, PJ *P) {
    PJ_LP lp;

    // Only implemented for ADAMS_WS2
    // Uses Newton-Raphson method on the following pair of functions:
    //      f_x(lam,phi) = adams_forward(lam, phi).x - xy.x
    //      f_y(lam,phi) = adams_forward(lam, phi).y - xy.y

    // Initial guess (very rough, especially at high northings)
    // The magic values are got with:
    //  echo 0   90 | src/proj -f "%.8f" +proj=adams_ws2 +R=1
    //  echo 180 0  | src/proj -f "%.8f" +proj=adams_ws2 +R=1
    lp.phi = std::max(std::min(xy.y / 2.62181347, 1.0), -1.0) * M_HALFPI;
    lp.lam =
        fabs(lp.phi) >= M_HALFPI
            ? 0
            : std::max(std::min(xy.x / 2.62205760 / cos(lp.phi), 1.0), -1.0) *
                  M_PI;

    constexpr double deltaXYTolerance = 1e-10;
    return pj_generic_inverse_2d(xy, P, lp, deltaXYTolerance);
}

static PJ_LP peirce_q_square_inverse(PJ_XY xy, PJ *P) {
    /* Heuristics based on trial and repeat process */
    PJ_LP lp;
    lp.phi = 0;
    if (xy.x == 0 && xy.y < 0) {
        lp.lam = -M_PI / 4;
        if (fabs(xy.y) < 2.622057580396)
            lp.phi = M_PI / 4;
    } else if (xy.x > 0 && fabs(xy.y) < 1e-7)
        lp.lam = M_PI / 4;
    else if (xy.x < 0 && fabs(xy.y) < 1e-7) {
        lp.lam = -3 * M_PI / 4;
        lp.phi = M_PI / 2 / 2.622057574224 * xy.x + M_PI / 2;
    } else if (fabs(xy.x) < 1e-7 && xy.y > 0)
        lp.lam = 3 * M_PI / 4;
    else if (xy.x >= 0 && xy.y <= 0) {
        lp.lam = 0;
        if (xy.x == 0 && xy.y == 0) {
            lp.phi = M_PI / 2;
            return lp;
        }
    } else if (xy.x >= 0 && xy.y >= 0)
        lp.lam = M_PI / 2;
    else if (xy.x <= 0 && xy.y >= 0) {
        if (fabs(xy.x) < fabs(xy.y))
            lp.lam = M_PI * 0.9;
        else
            lp.lam = -M_PI * 0.9;
    } else /* if( xy.x <= 0 && xy.y <= 0 ) */
        lp.lam = -M_PI / 2;

    constexpr double deltaXYTolerance = 1e-10;
    return pj_generic_inverse_2d(xy, P, lp, deltaXYTolerance);
}

static PJ_LP peirce_q_diamond_inverse(PJ_XY xy, PJ *P) {
    /* Heuristics based on a trial and repeat process */
    PJ_LP lp;
    lp.phi = 0;
    if (xy.x >= 0 && xy.y <= 0) {
        lp.lam = M_PI / 4;
        if (xy.x > 0 && xy.y == 0) {
            lp.lam = M_PI / 2;
            lp.phi = 0;
        } else if (xy.x == 0 && xy.y == 0) {
            lp.lam = 0;
            lp.phi = M_PI / 2;
            return lp;
        } else if (xy.x == 0 && xy.y < 0) {
            lp.lam = 0;
            lp.phi = M_PI / 4;
        }
    } else if (xy.x >= 0 && xy.y >= 0)
        lp.lam = 3 * M_PI / 4;
    else if (xy.x <= 0 && xy.y >= 0) {
        lp.lam = -3 * M_PI / 4;
    } else /* if( xy.x <= 0 && xy.y <= 0 ) */
        lp.lam = -M_PI / 4;

    if (fabs(xy.x) > 1.8540746773013719 + 1e-3 ||
        fabs(xy.y) > 1.8540746773013719 + 1e-3) {
        lp.phi = -M_PI / 4;
    }

    constexpr double deltaXYTolerance = 1e-10;
    return pj_generic_inverse_2d(xy, P, lp, deltaXYTolerance);
}

static PJ *pj_adams_setup(PJ *P, projection_type mode) {
    struct pj_adams_data *Q = static_cast<struct pj_adams_data *>(
        calloc(1, sizeof(struct pj_adams_data)));

    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    P->es = 0;
    P->fwd = adams_forward;

    Q->mode = mode;
    if (mode == ADAMS_WS2)
        P->inv = adams_inverse;

    if (mode == PEIRCE_Q) {
        // Quincuncial projections shape options: square, diamond, hemisphere,
        // horizontal (rectangle) or vertical (rectangle)
        const char *pqshape = pj_param(P->ctx, P->params, "sshape").s;

        if (!pqshape)
            pqshape = "diamond"; /* default if shape value not supplied */

        if (strcmp(pqshape, "square") == 0) {
            Q->pqshape = PEIRCE_Q_SQUARE;
            P->inv = peirce_q_square_inverse;
        } else if (strcmp(pqshape, "diamond") == 0) {
            Q->pqshape = PEIRCE_Q_DIAMOND;
            P->inv = peirce_q_diamond_inverse;
        } else if (strcmp(pqshape, "nhemisphere") == 0) {
            Q->pqshape = PEIRCE_Q_NHEMISPHERE;
        } else if (strcmp(pqshape, "shemisphere") == 0) {
            Q->pqshape = PEIRCE_Q_SHEMISPHERE;
        } else if (strcmp(pqshape, "horizontal") == 0) {
            Q->pqshape = PEIRCE_Q_HORIZONTAL;
            if (pj_param(P->ctx, P->params, "tscrollx").i) {
                double scrollx;
                scrollx = pj_param(P->ctx, P->params, "dscrollx").f;
                if (scrollx > 1 || scrollx < -1) {
                    proj_log_error(
                        P, _("Invalid value for scrollx: |scrollx| should "
                             "between -1 and 1"));
                    return pj_default_destructor(
                        P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
                }
                Q->scrollx = scrollx;
            }
        } else if (strcmp(pqshape, "vertical") == 0) {
            Q->pqshape = PEIRCE_Q_VERTICAL;
            if (pj_param(P->ctx, P->params, "tscrolly").i) {
                double scrolly;
                scrolly = pj_param(P->ctx, P->params, "dscrolly").f;
                if (scrolly > 1 || scrolly < -1) {
                    proj_log_error(
                        P, _("Invalid value for scrolly: |scrolly| should "
                             "between -1 and 1"));
                    return pj_default_destructor(
                        P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
                }
                Q->scrolly = scrolly;
            }
        } else {
            proj_log_error(P,
                           _("peirce_q: invalid value for 'shape' parameter"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    return P;
}

PJ *PJ_PROJECTION(guyou) { return pj_adams_setup(P, GUYOU); }

PJ *PJ_PROJECTION(peirce_q) { return pj_adams_setup(P, PEIRCE_Q); }

PJ *PJ_PROJECTION(adams_hemi) { return pj_adams_setup(P, ADAMS_HEMI); }

PJ *PJ_PROJECTION(adams_ws1) { return pj_adams_setup(P, ADAMS_WS1); }

PJ *PJ_PROJECTION(adams_ws2) { return pj_adams_setup(P, ADAMS_WS2); }

#undef TOL
#undef RSQRT2
