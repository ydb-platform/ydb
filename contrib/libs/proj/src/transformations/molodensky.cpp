/***********************************************************************

                  (Abridged) Molodensky Transform

                    Kristian Evers, 2017-07-07

************************************************************************

    Implements the (abridged) Molodensky transformations for 2D and 3D
    data.

    Primarily useful for implementation of datum shifts in transformation
    pipelines.

    The code in this file is mostly based on

        The Standard and Abridged Molodensky Coordinate Transformation
        Formulae, 2004, R.E. Deakin,
        http://www.mygeodesy.id.au/documents/Molodensky%20V2.pdf



************************************************************************
* Copyright (c) 2017, Kristian Evers / SDFE
*
* Permission is hereby granted, free of charge, to any person obtaining a
* copy of this software and associated documentation files (the "Software"),
* to deal in the Software without restriction, including without limitation
* the rights to use, copy, modify, merge, publish, distribute, sublicense,
* and/or sell copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included
* in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
* OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
* DEALINGS IN THE SOFTWARE.
*
***********************************************************************/

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(molodensky, "Molodensky transform");

static PJ_XYZ pj_molodensky_forward_3d(PJ_LPZ lpz, PJ *P);
static PJ_LPZ pj_molodensky_reverse_3d(PJ_XYZ xyz, PJ *P);

namespace { // anonymous namespace
struct pj_opaque_molodensky {
    double dx;
    double dy;
    double dz;
    double da;
    double df;
    int abridged;
};
} // anonymous namespace

static double RN(double a, double es, double phi) {
    /**********************************************************
        N(phi) - prime vertical radius of curvature
        -------------------------------------------

        This is basically the same function as in PJ_cart.c
        should probably be refactored into it's own file at some
        point.

    **********************************************************/
    double s = sin(phi);
    if (es == 0)
        return a;

    return a / sqrt(1 - es * s * s);
}

static double RM(double a, double es, double phi) {
    /**********************************************************
        M(phi) - Meridian radius of curvature
        -------------------------------------

        Source:

            E.J Krakiwsky & D.B. Thomson, 1974,
            GEODETIC POSITION COMPUTATIONS,

            Fredericton NB, Canada:
            University of New Brunswick,
            Department of Geodesy and Geomatics Engineering,
            Lecture Notes No. 39,
            99 pp.

            http://www2.unb.ca/gge/Pubs/LN39.pdf

    **********************************************************/
    double s = sin(phi);
    if (es == 0)
        return a;

    /* eq. 13a */
    if (phi == 0)
        return a * (1 - es);

    /* eq. 13b */
    if (fabs(phi) == M_PI_2)
        return a / sqrt(1 - es);

    /* eq. 13 */
    return (a * (1 - es)) / pow(1 - es * s * s, 1.5);
}

static PJ_LPZ calc_standard_params(PJ_LPZ lpz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *)P->opaque;
    double dphi, dlam, dh;

    /* sines and cosines */
    double slam = sin(lpz.lam);
    double clam = cos(lpz.lam);
    double sphi = sin(lpz.phi);
    double cphi = cos(lpz.phi);

    /* ellipsoid parameters and differences */
    double f = P->f, a = P->a;
    double dx = Q->dx, dy = Q->dy, dz = Q->dz;
    double da = Q->da, df = Q->df;

    /* ellipsoid radii of curvature */
    double rho = RM(a, P->es, lpz.phi);
    double nu = RN(a, P->es, lpz.phi);

    /* delta phi */
    dphi = (-dx * sphi * clam) - (dy * sphi * slam) + (dz * cphi) +
           ((nu * P->es * sphi * cphi * da) / a) +
           (sphi * cphi * (rho / (1 - f) + nu * (1 - f)) * df);
    const double dphi_denom = rho + lpz.z;
    if (dphi_denom == 0.0) {
        lpz.lam = HUGE_VAL;
        return lpz;
    }
    dphi /= dphi_denom;

    /* delta lambda */
    const double dlam_denom = (nu + lpz.z) * cphi;
    if (dlam_denom == 0.0) {
        lpz.lam = HUGE_VAL;
        return lpz;
    }
    dlam = (-dx * slam + dy * clam) / dlam_denom;

    /* delta h */
    dh = dx * cphi * clam + dy * cphi * slam + dz * sphi - (a / nu) * da +
         nu * (1 - f) * sphi * sphi * df;

    lpz.phi = dphi;
    lpz.lam = dlam;
    lpz.z = dh;

    return lpz;
}

static PJ_LPZ calc_abridged_params(PJ_LPZ lpz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *)P->opaque;
    double dphi, dlam, dh;

    /* sines and cosines */
    double slam = sin(lpz.lam);
    double clam = cos(lpz.lam);
    double sphi = sin(lpz.phi);
    double cphi = cos(lpz.phi);

    /* ellipsoid parameters and differences */
    double dx = Q->dx, dy = Q->dy, dz = Q->dz;
    double da = Q->da, df = Q->df;
    double adffda = (P->a * df + P->f * da);

    /* delta phi */
    dphi = -dx * sphi * clam - dy * sphi * slam + dz * cphi +
           adffda * sin(2 * lpz.phi);
    dphi /= RM(P->a, P->es, lpz.phi);

    /* delta lambda */
    dlam = -dx * slam + dy * clam;
    const double dlam_denom = RN(P->a, P->es, lpz.phi) * cphi;
    if (dlam_denom == 0.0) {
        lpz.lam = HUGE_VAL;
        return lpz;
    }
    dlam /= dlam_denom;

    /* delta h */
    dh = dx * cphi * clam + dy * cphi * slam + dz * sphi - da +
         adffda * sphi * sphi;

    /* offset coordinate */
    lpz.phi = dphi;
    lpz.lam = dlam;
    lpz.z = dh;

    return lpz;
}

static PJ_XY pj_molodensky_forward_2d(PJ_LP lp, PJ *P) {
    PJ_COORD point = {{0, 0, 0, 0}};

    point.lp = lp;
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto xyz = pj_molodensky_forward_3d(point.lpz, P);
    point.xyz = xyz;

    return point.xy;
}

static PJ_LP pj_molodensky_reverse_2d(PJ_XY xy, PJ *P) {
    PJ_COORD point = {{0, 0, 0, 0}};

    point.xy = xy;
    point.xyz.z = 0;
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto lpz = pj_molodensky_reverse_3d(point.xyz, P);
    point.lpz = lpz;

    return point.lp;
}

static PJ_XYZ pj_molodensky_forward_3d(PJ_LPZ lpz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *)P->opaque;
    PJ_COORD point = {{0, 0, 0, 0}};

    point.lpz = lpz;

    /* calculate parameters depending on the mode we are in */
    if (Q->abridged) {
        lpz = calc_abridged_params(lpz, P);
    } else {
        lpz = calc_standard_params(lpz, P);
    }
    if (lpz.lam == HUGE_VAL) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().xyz;
    }

    /* offset coordinate */
    point.lpz.phi += lpz.phi;
    point.lpz.lam += lpz.lam;
    point.lpz.z += lpz.z;

    return point.xyz;
}

static void pj_molodensky_forward_4d(PJ_COORD &obs, PJ *P) {
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto xyz = pj_molodensky_forward_3d(obs.lpz, P);
    obs.xyz = xyz;
}

static PJ_LPZ pj_molodensky_reverse_3d(PJ_XYZ xyz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *)P->opaque;
    PJ_COORD point = {{0, 0, 0, 0}};
    PJ_LPZ lpz;

    /* calculate parameters depending on the mode we are in */
    point.xyz = xyz;
    if (Q->abridged)
        lpz = calc_abridged_params(point.lpz, P);
    else
        lpz = calc_standard_params(point.lpz, P);

    if (lpz.lam == HUGE_VAL) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().lpz;
    }

    /* offset coordinate */
    point.lpz.phi -= lpz.phi;
    point.lpz.lam -= lpz.lam;
    point.lpz.z -= lpz.z;

    return point.lpz;
}

static void pj_molodensky_reverse_4d(PJ_COORD &obs, PJ *P) {
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto lpz = pj_molodensky_reverse_3d(obs.xyz, P);
    obs.lpz = lpz;
}

PJ *PJ_TRANSFORMATION(molodensky, 1) {
    struct pj_opaque_molodensky *Q = static_cast<struct pj_opaque_molodensky *>(
        calloc(1, sizeof(struct pj_opaque_molodensky)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = (void *)Q;

    P->fwd4d = pj_molodensky_forward_4d;
    P->inv4d = pj_molodensky_reverse_4d;
    P->fwd3d = pj_molodensky_forward_3d;
    P->inv3d = pj_molodensky_reverse_3d;
    P->fwd = pj_molodensky_forward_2d;
    P->inv = pj_molodensky_reverse_2d;

    P->left = PJ_IO_UNITS_RADIANS;
    P->right = PJ_IO_UNITS_RADIANS;

    /* read args */
    if (!pj_param(P->ctx, P->params, "tdx").i) {
        proj_log_error(P, _("missing dx"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->dx = pj_param(P->ctx, P->params, "ddx").f;

    if (!pj_param(P->ctx, P->params, "tdy").i) {
        proj_log_error(P, _("missing dy"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->dy = pj_param(P->ctx, P->params, "ddy").f;

    if (!pj_param(P->ctx, P->params, "tdz").i) {
        proj_log_error(P, _("missing dz"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->dz = pj_param(P->ctx, P->params, "ddz").f;

    if (!pj_param(P->ctx, P->params, "tda").i) {
        proj_log_error(P, _("missing da"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->da = pj_param(P->ctx, P->params, "dda").f;

    if (!pj_param(P->ctx, P->params, "tdf").i) {
        proj_log_error(P, _("missing df"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->df = pj_param(P->ctx, P->params, "ddf").f;

    Q->abridged = pj_param(P->ctx, P->params, "tabridged").i;

    return P;
}
