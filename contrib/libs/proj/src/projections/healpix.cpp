/******************************************************************************
 * Project: PROJ.4
 * Purpose: Implementation of the HEALPix and rHEALPix projections.
 *          For background see
 *<http://code.scenzgrid.org/index.php/p/scenzgrid-py/source/tree/master/docs/rhealpix_dggs.pdf>.
 * Authors: Alex Raichev (raichev@cs.auckland.ac.nz)
 *          Michael Speth (spethm@landcareresearch.co.nz)
 * Notes:   Raichev implemented these projections in Python and
 *          Speth translated them into C here.
 ******************************************************************************
 * Copyright (c) 2001, Thomas Flemming, tf@ttqv.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substcounteral portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *****************************************************************************/

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(healpix, "HEALPix") "\n\tSph&Ell\n\trot_xy=";
PROJ_HEAD(rhealpix, "rHEALPix") "\n\tSph&Ell\n\tnorth_square= south_square=";

/* Matrix for counterclockwise rotation by pi/2: */
#define R1                                                                     \
    {                                                                          \
        {0, -1}, { 1, 0 }                                                      \
    }
/* Matrix for counterclockwise rotation by pi: */
#define R2                                                                     \
    {                                                                          \
        {-1, 0}, { 0, -1 }                                                     \
    }
/* Matrix for counterclockwise rotation by 3*pi/2:  */
#define R3                                                                     \
    {                                                                          \
        {0, 1}, { -1, 0 }                                                      \
    }
/* Identity matrix */
#define IDENT                                                                  \
    {                                                                          \
        {1, 0}, { 0, 1 }                                                       \
    }
/* IDENT, R1, R2, R3, R1 inverse, R2 inverse, R3 inverse:*/
#define ROT                                                                    \
    { IDENT, R1, R2, R3, R3, R2, R1 }
/* Fuzz to handle rounding errors: */
#define EPS 1e-15

namespace { // anonymous namespace
struct pj_healpix_data {
    int north_square;
    int south_square;
    double rot_xy;
    double qp;
    double *apa;
};
} // anonymous namespace

typedef struct {
    int cn;      /* An integer 0--3 indicating the position of the polar cap. */
    double x, y; /* Coordinates of the pole point (point of most extreme
                    latitude on the polar caps). */
    enum Region { north, south, equatorial } region;
} CapMap;

static const double rot[7][2][2] = ROT;

/**
 * Returns the sign of the double.
 * @param v the parameter whose sign is returned.
 * @return 1 for positive number, -1 for negative, and 0 for zero.
 **/
static double sign(double v) { return v > 0 ? 1 : (v < 0 ? -1 : 0); }

static PJ_XY rotate(PJ_XY p, double angle) {
    PJ_XY result;
    result.x = p.x * cos(angle) - p.y * sin(angle);
    result.y = p.y * cos(angle) + p.x * sin(angle);
    return result;
}

/**
 * Return the index of the matrix in ROT.
 * @param index ranges from -3 to 3.
 */
static int get_rotate_index(int index) {
    switch (index) {
    case 0:
        return 0;
    case 1:
        return 1;
    case 2:
        return 2;
    case 3:
        return 3;
    case -1:
        return 4;
    case -2:
        return 5;
    case -3:
        return 6;
    }
    return 0;
}

/**
 * Return 1 if point (testx, testy) lies in the interior of the polygon
 * determined by the vertices in vert, and return 0 otherwise.
 * See http://paulbourke.net/geometry/polygonmesh/ for more details.
 * @param nvert the number of vertices in the polygon.
 * @param vert the (x, y)-coordinates of the polygon's vertices
 **/
static int pnpoly(int nvert, double vert[][2], double testx, double testy) {
    int i;
    int counter = 0;
    double xinters;
    PJ_XY p1, p2;

    /* Check for boundary cases */
    for (i = 0; i < nvert; i++) {
        if (testx == vert[i][0] && testy == vert[i][1]) {
            return 1;
        }
    }

    p1.x = vert[0][0];
    p1.y = vert[0][1];

    for (i = 1; i < nvert; i++) {
        p2.x = vert[i % nvert][0];
        p2.y = vert[i % nvert][1];
        if (testy > MIN(p1.y, p2.y) && testy <= MAX(p1.y, p2.y) &&
            testx <= MAX(p1.x, p2.x) && p1.y != p2.y) {
            xinters = (testy - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x;
            if (p1.x == p2.x || testx <= xinters)
                counter++;
        }
        p1 = p2;
    }

    if (counter % 2 == 0) {
        return 0;
    } else {
        return 1;
    }
}

/**
 * Return 1 if (x, y) lies in (the interior or boundary of) the image of the
 * HEALPix projection (in case proj=0) or in the image the rHEALPix projection
 * (in case proj=1), and return 0 otherwise.
 * @param north_square the position of the north polar square (rHEALPix only)
 * @param south_square the position of the south polar square (rHEALPix only)
 **/
static int in_image(double x, double y, int proj, int north_square,
                    int south_square) {
    if (proj == 0) {
        double healpixVertsJit[][2] = {{-M_PI - EPS, M_FORTPI},
                                       {-3 * M_FORTPI, M_HALFPI + EPS},
                                       {-M_HALFPI, M_FORTPI + EPS},
                                       {-M_FORTPI, M_HALFPI + EPS},
                                       {0.0, M_FORTPI + EPS},
                                       {M_FORTPI, M_HALFPI + EPS},
                                       {M_HALFPI, M_FORTPI + EPS},
                                       {3 * M_FORTPI, M_HALFPI + EPS},
                                       {M_PI + EPS, M_FORTPI},
                                       {M_PI + EPS, -M_FORTPI},
                                       {3 * M_FORTPI, -M_HALFPI - EPS},
                                       {M_HALFPI, -M_FORTPI - EPS},
                                       {M_FORTPI, -M_HALFPI - EPS},
                                       {0.0, -M_FORTPI - EPS},
                                       {-M_FORTPI, -M_HALFPI - EPS},
                                       {-M_HALFPI, -M_FORTPI - EPS},
                                       {-3 * M_FORTPI, -M_HALFPI - EPS},
                                       {-M_PI - EPS, -M_FORTPI},
                                       {-M_PI - EPS, M_FORTPI}};
        return pnpoly((int)sizeof(healpixVertsJit) / sizeof(healpixVertsJit[0]),
                      healpixVertsJit, x, y);
    } else {
        /**
         * We need to assign the array this way because the input is
         * dynamic (north_square and south_square vars are unknown at
         * compile time).
         **/
        double rhealpixVertsJit[][2] = {
            {-M_PI - EPS, M_FORTPI + EPS},
            {-M_PI + north_square * M_HALFPI - EPS, M_FORTPI + EPS},
            {-M_PI + north_square * M_HALFPI - EPS, 3 * M_FORTPI + EPS},
            {-M_PI + (north_square + 1.0) * M_HALFPI + EPS, 3 * M_FORTPI + EPS},
            {-M_PI + (north_square + 1.0) * M_HALFPI + EPS, M_FORTPI + EPS},
            {M_PI + EPS, M_FORTPI + EPS},
            {M_PI + EPS, -M_FORTPI - EPS},
            {-M_PI + (south_square + 1.0) * M_HALFPI + EPS, -M_FORTPI - EPS},
            {-M_PI + (south_square + 1.0) * M_HALFPI + EPS,
             -3 * M_FORTPI - EPS},
            {-M_PI + south_square * M_HALFPI - EPS, -3 * M_FORTPI - EPS},
            {-M_PI + south_square * M_HALFPI - EPS, -M_FORTPI - EPS},
            {-M_PI - EPS, -M_FORTPI - EPS}};

        return pnpoly((int)sizeof(rhealpixVertsJit) /
                          sizeof(rhealpixVertsJit[0]),
                      rhealpixVertsJit, x, y);
    }
}

/**
 * Return the authalic latitude of latitude alpha (if inverse=0) or
 * return the latitude of authalic latitude alpha (if inverse=1).
 * P contains the relevant ellipsoid parameters.
 **/
static double auth_lat(PJ *P, double alpha, int inverse) {
    const struct pj_healpix_data *Q =
        static_cast<const struct pj_healpix_data *>(P->opaque);
    if (inverse == 0) {
        /* Authalic latitude from geographic latitude. */
        return pj_authalic_lat(alpha, sin(alpha), cos(alpha), Q->apa, P, Q->qp);
    } else {
        /* Geographic latitude from authalic latitude. */
        return pj_authalic_lat_inverse(alpha, Q->apa, P, Q->qp);
    }
}

/**
 * Return the HEALPix projection of the longitude-latitude point lp on
 * the unit sphere.
 **/
static PJ_XY healpix_sphere(PJ_LP lp) {
    double lam = lp.lam;
    double phi = lp.phi;
    double phi0 = asin(2.0 / 3.0);
    PJ_XY xy;

    /* equatorial region */
    if (fabs(phi) <= phi0) {
        xy.x = lam;
        xy.y = 3 * M_PI / 8 * sin(phi);
    } else {
        double lamc;
        double sigma = sqrt(3 * (1 - fabs(sin(phi))));
        double cn = floor(2 * lam / M_PI + 2);
        if (cn >= 4) {
            cn = 3;
        }
        lamc = -3 * M_FORTPI + M_HALFPI * cn;
        xy.x = lamc + (lam - lamc) * sigma;
        xy.y = sign(phi) * M_FORTPI * (2 - sigma);
    }
    return xy;
}

/**
 * Return the inverse of healpix_sphere().
 **/
static PJ_LP healpix_spherhealpix_e_inverse(PJ_XY xy) {
    PJ_LP lp;
    double x = xy.x;
    double y = xy.y;
    double y0 = M_FORTPI;

    /* Equatorial region. */
    if (fabs(y) <= y0) {
        lp.lam = x;
        lp.phi = asin(8 * y / (3 * M_PI));
    } else if (fabs(y) < M_HALFPI) {
        double cn = floor(2 * x / M_PI + 2);
        double xc, tau;
        if (cn >= 4) {
            cn = 3;
        }
        xc = -3 * M_FORTPI + M_HALFPI * cn;
        tau = 2.0 - 4 * fabs(y) / M_PI;
        lp.lam = xc + (x - xc) / tau;
        lp.phi = sign(y) * asin(1.0 - pow(tau, 2) / 3.0);
    } else {
        lp.lam = -M_PI;
        lp.phi = sign(y) * M_HALFPI;
    }
    return (lp);
}

/**
 * Return the vector sum a + b, where a and b are 2-dimensional vectors.
 * @param ret holds a + b.
 **/
static void vector_add(const double a[2], const double b[2], double *ret) {
    int i;
    for (i = 0; i < 2; i++) {
        ret[i] = a[i] + b[i];
    }
}

/**
 * Return the vector difference a - b, where a and b are 2-dimensional vectors.
 * @param ret holds a - b.
 **/
static void vector_sub(const double a[2], const double b[2], double *ret) {
    int i;
    for (i = 0; i < 2; i++) {
        ret[i] = a[i] - b[i];
    }
}

/**
 * Return the 2 x 1 matrix product a*b, where a is a 2 x 2 matrix and
 * b is a 2 x 1 matrix.
 * @param ret holds a*b.
 **/
static void dot_product(const double a[2][2], const double b[2], double *ret) {
    int i, j;
    int length = 2;
    for (i = 0; i < length; i++) {
        ret[i] = 0;
        for (j = 0; j < length; j++) {
            ret[i] += a[i][j] * b[j];
        }
    }
}

/**
 * Return the number of the polar cap, the pole point coordinates, and
 * the region that (x, y) lies in.
 * If inverse=0, then assume (x,y) lies in the image of the HEALPix
 * projection of the unit sphere.
 * If inverse=1, then assume (x,y) lies in the image of the
 * (north_square, south_square)-rHEALPix projection of the unit sphere.
 **/
static CapMap get_cap(double x, double y, int north_square, int south_square,
                      int inverse) {
    CapMap capmap;
    double c;

    capmap.x = x;
    capmap.y = y;
    if (inverse == 0) {
        if (y > M_FORTPI) {
            capmap.region = CapMap::north;
            c = M_HALFPI;
        } else if (y < -M_FORTPI) {
            capmap.region = CapMap::south;
            c = -M_HALFPI;
        } else {
            capmap.region = CapMap::equatorial;
            capmap.cn = 0;
            return capmap;
        }
        /* polar region */
        if (x < -M_HALFPI) {
            capmap.cn = 0;
            capmap.x = (-3 * M_FORTPI);
            capmap.y = c;
        } else if (x >= -M_HALFPI && x < 0) {
            capmap.cn = 1;
            capmap.x = -M_FORTPI;
            capmap.y = c;
        } else if (x >= 0 && x < M_HALFPI) {
            capmap.cn = 2;
            capmap.x = M_FORTPI;
            capmap.y = c;
        } else {
            capmap.cn = 3;
            capmap.x = 3 * M_FORTPI;
            capmap.y = c;
        }
    } else {
        if (y > M_FORTPI) {
            capmap.region = CapMap::north;
            capmap.x = -3 * M_FORTPI + north_square * M_HALFPI;
            capmap.y = M_HALFPI;
            x = x - north_square * M_HALFPI;
        } else if (y < -M_FORTPI) {
            capmap.region = CapMap::south;
            capmap.x = -3 * M_FORTPI + south_square * M_HALFPI;
            capmap.y = -M_HALFPI;
            x = x - south_square * M_HALFPI;
        } else {
            capmap.region = CapMap::equatorial;
            capmap.cn = 0;
            return capmap;
        }
        /* Polar Region, find the HEALPix polar cap number that
           x, y moves to when rHEALPix polar square is disassembled. */
        if (capmap.region == CapMap::north) {
            if (y >= -x - M_FORTPI - EPS && y < x + 5 * M_FORTPI - EPS) {
                capmap.cn = (north_square + 1) % 4;
            } else if (y > -x - M_FORTPI + EPS && y >= x + 5 * M_FORTPI - EPS) {
                capmap.cn = (north_square + 2) % 4;
            } else if (y <= -x - M_FORTPI + EPS && y > x + 5 * M_FORTPI + EPS) {
                capmap.cn = (north_square + 3) % 4;
            } else {
                capmap.cn = north_square;
            }
        } else /* if (capmap.region == CapMap::south) */ {
            if (y <= x + M_FORTPI + EPS && y > -x - 5 * M_FORTPI + EPS) {
                capmap.cn = (south_square + 1) % 4;
            } else if (y < x + M_FORTPI - EPS && y <= -x - 5 * M_FORTPI + EPS) {
                capmap.cn = (south_square + 2) % 4;
            } else if (y >= x + M_FORTPI - EPS && y < -x - 5 * M_FORTPI - EPS) {
                capmap.cn = (south_square + 3) % 4;
            } else {
                capmap.cn = south_square;
            }
        }
    }
    return capmap;
}

/**
 * Rearrange point (x, y) in the HEALPix projection by
 * combining the polar caps into two polar squares.
 * Put the north polar square in position north_square and
 * the south polar square in position south_square.
 * If inverse=1, then uncombine the polar caps.
 * @param north_square integer between 0 and 3.
 * @param south_square integer between 0 and 3.
 **/
static PJ_XY combine_caps(double x, double y, int north_square,
                          int south_square, int inverse) {
    PJ_XY xy;
    double vector[2];
    double v_min_c[2];
    double ret_dot[2];
    const double(*tmpRot)[2];
    int pole = 0;

    CapMap capmap = get_cap(x, y, north_square, south_square, inverse);
    if (capmap.region == CapMap::equatorial) {
        xy.x = capmap.x;
        xy.y = capmap.y;
        return xy;
    }

    double v[] = {x, y};
    double c[] = {capmap.x, capmap.y};

    if (inverse == 0) {
        /* Rotate (x, y) about its polar cap tip and then translate it to
           north_square or south_square. */

        if (capmap.region == CapMap::north) {
            pole = north_square;
            tmpRot = rot[get_rotate_index(capmap.cn - pole)];
        } else {
            pole = south_square;
            tmpRot = rot[get_rotate_index(-1 * (capmap.cn - pole))];
        }
    } else {
        /* Inverse function.
         Unrotate (x, y) and then translate it back. */

        /* disassemble */
        if (capmap.region == CapMap::north) {
            pole = north_square;
            tmpRot = rot[get_rotate_index(-1 * (capmap.cn - pole))];
        } else {
            pole = south_square;
            tmpRot = rot[get_rotate_index(capmap.cn - pole)];
        }
    }

    vector_sub(v, c, v_min_c);
    dot_product(tmpRot, v_min_c, ret_dot);
    {
        double a[] = {-3 * M_FORTPI +
                          ((inverse == 0) ? pole : capmap.cn) * M_HALFPI,
                      ((capmap.region == CapMap::north) ? 1 : -1) * M_HALFPI};
        vector_add(ret_dot, a, vector);
    }

    xy.x = vector[0];
    xy.y = vector[1];
    return xy;
}

static PJ_XY s_healpix_forward(PJ_LP lp, PJ *P) { /* sphere  */
    (void)P;
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);
    return rotate(healpix_sphere(lp), -Q->rot_xy);
}

static PJ_XY e_healpix_forward(PJ_LP lp, PJ *P) { /* ellipsoid  */
    lp.phi = auth_lat(P, lp.phi, 0);
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);
    return rotate(healpix_sphere(lp), -Q->rot_xy);
}

static PJ_LP s_healpix_inverse(PJ_XY xy, PJ *P) { /* sphere */
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);
    xy = rotate(xy, Q->rot_xy);

    /* Check whether (x, y) lies in the HEALPix image */
    if (in_image(xy.x, xy.y, 0, 0, 0) == 0) {
        PJ_LP lp;
        lp.lam = HUGE_VAL;
        lp.phi = HUGE_VAL;
        proj_context_errno_set(
            P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    return healpix_spherhealpix_e_inverse(xy);
}

static PJ_LP e_healpix_inverse(PJ_XY xy, PJ *P) { /* ellipsoid */
    PJ_LP lp = {0.0, 0.0};
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);
    xy = rotate(xy, Q->rot_xy);

    /* Check whether (x, y) lies in the HEALPix image. */
    if (in_image(xy.x, xy.y, 0, 0, 0) == 0) {
        lp.lam = HUGE_VAL;
        lp.phi = HUGE_VAL;
        proj_context_errno_set(
            P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    lp = healpix_spherhealpix_e_inverse(xy);
    lp.phi = auth_lat(P, lp.phi, 1);
    return lp;
}

static PJ_XY s_rhealpix_forward(PJ_LP lp, PJ *P) { /* sphere */
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);

    PJ_XY xy = healpix_sphere(lp);
    return combine_caps(xy.x, xy.y, Q->north_square, Q->south_square, 0);
}

static PJ_XY e_rhealpix_forward(PJ_LP lp, PJ *P) { /* ellipsoid */
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);
    PJ_XY xy;
    lp.phi = auth_lat(P, lp.phi, 0);
    xy = healpix_sphere(lp);
    return combine_caps(xy.x, xy.y, Q->north_square, Q->south_square, 0);
}

static PJ_LP s_rhealpix_inverse(PJ_XY xy, PJ *P) { /* sphere */
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);

    /* Check whether (x, y) lies in the rHEALPix image. */
    if (in_image(xy.x, xy.y, 1, Q->north_square, Q->south_square) == 0) {
        PJ_LP lp;
        lp.lam = HUGE_VAL;
        lp.phi = HUGE_VAL;
        proj_context_errno_set(
            P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    xy = combine_caps(xy.x, xy.y, Q->north_square, Q->south_square, 1);
    return healpix_spherhealpix_e_inverse(xy);
}

static PJ_LP e_rhealpix_inverse(PJ_XY xy, PJ *P) { /* ellipsoid */
    struct pj_healpix_data *Q =
        static_cast<struct pj_healpix_data *>(P->opaque);
    PJ_LP lp = {0.0, 0.0};

    /* Check whether (x, y) lies in the rHEALPix image. */
    if (in_image(xy.x, xy.y, 1, Q->north_square, Q->south_square) == 0) {
        lp.lam = HUGE_VAL;
        lp.phi = HUGE_VAL;
        proj_context_errno_set(
            P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    xy = combine_caps(xy.x, xy.y, Q->north_square, Q->south_square, 1);
    lp = healpix_spherhealpix_e_inverse(xy);
    lp.phi = auth_lat(P, lp.phi, 1);
    return lp;
}

static PJ *pj_healpix_data_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    free(static_cast<struct pj_healpix_data *>(P->opaque)->apa);
    return pj_default_destructor(P, errlev);
}

PJ *PJ_PROJECTION(healpix) {
    struct pj_healpix_data *Q = static_cast<struct pj_healpix_data *>(
        calloc(1, sizeof(struct pj_healpix_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_healpix_data_destructor;

    double angle = pj_param(P->ctx, P->params, "drot_xy").f;
    Q->rot_xy = PJ_TORAD(angle);

    if (P->es != 0.0) {
        Q->apa = pj_authalic_lat_compute_coeffs(P->n); /* For auth_lat(). */
        if (nullptr == Q->apa)
            return pj_healpix_data_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
        Q->qp = pj_authalic_lat_q(1.0, P); /* For auth_lat(). */
        P->a = P->a * sqrt(0.5 * Q->qp);   /* Set P->a to authalic radius. */
        pj_calc_ellipsoid_params(
            P, P->a, P->es); /* Ensure we have a consistent parameter set */
        P->fwd = e_healpix_forward;
        P->inv = e_healpix_inverse;
    } else {
        P->fwd = s_healpix_forward;
        P->inv = s_healpix_inverse;
    }

    return P;
}

PJ *PJ_PROJECTION(rhealpix) {
    struct pj_healpix_data *Q = static_cast<struct pj_healpix_data *>(
        calloc(1, sizeof(struct pj_healpix_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_healpix_data_destructor;

    Q->north_square = pj_param(P->ctx, P->params, "inorth_square").i;
    Q->south_square = pj_param(P->ctx, P->params, "isouth_square").i;

    /* Check for valid north_square and south_square inputs. */
    if (Q->north_square < 0 || Q->north_square > 3) {
        proj_log_error(
            P,
            _("Invalid value for north_square: it should be in [0,3] range."));
        return pj_healpix_data_destructor(
            P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (Q->south_square < 0 || Q->south_square > 3) {
        proj_log_error(
            P,
            _("Invalid value for south_square: it should be in [0,3] range."));
        return pj_healpix_data_destructor(
            P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (P->es != 0.0) {
        Q->apa = pj_authalic_lat_compute_coeffs(P->n); /* For auth_lat(). */
        if (nullptr == Q->apa)
            return pj_healpix_data_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
        Q->qp = pj_authalic_lat_q(1.0, P); /* For auth_lat(). */
        P->a = P->a * sqrt(0.5 * Q->qp);   /* Set P->a to authalic radius. */
        P->ra = 1.0 / P->a;
        P->fwd = e_rhealpix_forward;
        P->inv = e_rhealpix_inverse;
    } else {
        P->fwd = s_rhealpix_forward;
        P->inv = s_rhealpix_inverse;
    }

    return P;
}

#undef R1
#undef R2
#undef R3
#undef IDENT
#undef ROT
#undef EPS
