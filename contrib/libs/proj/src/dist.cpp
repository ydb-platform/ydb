/******************************************************************************
 * Project:  PROJ
 * Purpose:  Distance computation
 *
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2016-06-09/2016-11-06
 *
 ******************************************************************************
 * Copyright (c) 2016, 2017 Thomas Knudsen/SDFE
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
 *****************************************************************************/

#define FROM_PROJ_CPP

#include "geodesic.h"

#include "proj.h"
#include "proj_internal.h"
#include <math.h>

#include "proj/internal/io_internal.hpp"

/* Geodesic distance (in meter) + fwd and rev azimuth between two points on the
 * ellipsoid */
PJ_COORD proj_geod(const PJ *P, PJ_COORD a, PJ_COORD b) {
    PJ_COORD c;
    if (!P->geod) {
        return proj_coord_error();
    }
    /* Note: the geodesic code takes arguments in degrees */
    geod_inverse(P->geod, PJ_TODEG(a.lpz.phi), PJ_TODEG(a.lpz.lam),
                 PJ_TODEG(b.lpz.phi), PJ_TODEG(b.lpz.lam), c.v, c.v + 1,
                 c.v + 2);

    // cppcheck-suppress uninitvar
    return c;
}

PJ_COORD proj_geod_direct(const PJ *P, PJ_COORD a, double azimuth,
                          double distance) {
    if (!P->geod) {
        return proj_coord_error();
    }

    double lat = 0, lon = 0, azi = 0;
    geod_direct(P->geod, PJ_TODEG(a.lpz.phi), PJ_TODEG(a.lpz.lam),
                PJ_TODEG(azimuth), distance, &lat, &lon, &azi);

    return proj_coord(PJ_TORAD(lon), PJ_TORAD(lat), PJ_TORAD(azi), 0);
}

/* Geodesic distance (in meter) between two points with angular 2D coordinates
 */
double proj_lp_dist(const PJ *P, PJ_COORD a, PJ_COORD b) {
    double s12, azi1, azi2;
    /* Note: the geodesic code takes arguments in degrees */
    if (!P->geod) {
        return HUGE_VAL;
    }
    geod_inverse(P->geod, PJ_TODEG(a.lpz.phi), PJ_TODEG(a.lpz.lam),
                 PJ_TODEG(b.lpz.phi), PJ_TODEG(b.lpz.lam), &s12, &azi1, &azi2);
    return s12;
}

/* The geodesic distance AND the vertical offset */
double proj_lpz_dist(const PJ *P, PJ_COORD a, PJ_COORD b) {
    if (HUGE_VAL == a.lpz.lam || HUGE_VAL == b.lpz.lam)
        return HUGE_VAL;
    return hypot(proj_lp_dist(P, a, b), a.lpz.z - b.lpz.z);
}

/* Euclidean distance between two points with linear 2D coordinates */
double proj_xy_dist(PJ_COORD a, PJ_COORD b) {
    return hypot(a.xy.x - b.xy.x, a.xy.y - b.xy.y);
}

/* Euclidean distance between two points with linear 3D coordinates */
double proj_xyz_dist(PJ_COORD a, PJ_COORD b) {
    return hypot(proj_xy_dist(a, b), a.xyz.z - b.xyz.z);
}
