/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Stub projection implementation for lat/long coordinates. We
 *           don't actually change the coordinates, but we want proj=latlong
 *           to act sort of like a projection.
 * Author:   Frank Warmerdam, warmerdam@pobox.com
 *
 ******************************************************************************
 * Copyright (c) 2000, Frank Warmerdam
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

/* very loosely based upon DMA code by Bradford W. Drew */

#include "proj_internal.h"

PROJ_HEAD(lonlat, "Lat/long (Geodetic)") "\n\t";
PROJ_HEAD(latlon, "Lat/long (Geodetic alias)") "\n\t";
PROJ_HEAD(latlong, "Lat/long (Geodetic alias)") "\n\t";
PROJ_HEAD(longlat, "Lat/long (Geodetic alias)") "\n\t";

static PJ_XY latlong_forward(PJ_LP lp, PJ *P) {
    PJ_XY xy = {0.0, 0.0};
    (void)P;
    xy.x = lp.lam;
    xy.y = lp.phi;
    return xy;
}

static PJ_LP latlong_inverse(PJ_XY xy, PJ *P) {
    PJ_LP lp = {0.0, 0.0};
    (void)P;
    lp.phi = xy.y;
    lp.lam = xy.x;
    return lp;
}

static PJ_XYZ latlong_forward_3d(PJ_LPZ lpz, PJ *P) {
    PJ_XYZ xyz = {0, 0, 0};
    (void)P;
    xyz.x = lpz.lam;
    xyz.y = lpz.phi;
    xyz.z = lpz.z;
    return xyz;
}

static PJ_LPZ latlong_inverse_3d(PJ_XYZ xyz, PJ *P) {
    PJ_LPZ lpz = {0, 0, 0};
    (void)P;
    lpz.lam = xyz.x;
    lpz.phi = xyz.y;
    lpz.z = xyz.z;
    return lpz;
}

static void latlong_forward_4d(PJ_COORD &, PJ *) {}

static void latlong_inverse_4d(PJ_COORD &, PJ *) {}

static PJ *latlong_setup(PJ *P) {
    P->is_latlong = 1;
    P->x0 = 0;
    P->y0 = 0;
    P->inv = latlong_inverse;
    P->fwd = latlong_forward;
    P->inv3d = latlong_inverse_3d;
    P->fwd3d = latlong_forward_3d;
    P->inv4d = latlong_inverse_4d;
    P->fwd4d = latlong_forward_4d;
    P->left = PJ_IO_UNITS_RADIANS;
    P->right = PJ_IO_UNITS_RADIANS;
    return P;
}

PJ *PJ_PROJECTION(latlong) { return latlong_setup(P); }

PJ *PJ_PROJECTION(longlat) { return latlong_setup(P); }

PJ *PJ_PROJECTION(latlon) { return latlong_setup(P); }

PJ *PJ_PROJECTION(lonlat) { return latlong_setup(P); }
