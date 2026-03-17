/************************************************************************
 * Copyright (c) 2022, Even Rouault <even.rouault at spatialys.com>
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

PROJ_HEAD(vertoffset, "Vertical Offset and Slope")
"\n\tTransformation"
    "\n\tlat_0= lon_0= dh= slope_lat= slope_lon=";

namespace { // anonymous namespace
struct pj_opaque_vertoffset {
    double slope_lon;
    double slope_lat;
    double zoff;
    double rho0;
    double nu0;
};
} // anonymous namespace

// Cf EPSG Dataset coordinate operation method code 1046 "Vertical Offset and
// Slope"

static double get_forward_offset(const PJ *P, double phi, double lam) {
    const struct pj_opaque_vertoffset *Q =
        (const struct pj_opaque_vertoffset *)P->opaque;
    return Q->zoff + Q->slope_lat * Q->rho0 * (phi - P->phi0) +
           Q->slope_lon * Q->nu0 * lam * cos(phi);
}

static PJ_XYZ forward_3d(PJ_LPZ lpz, PJ *P) {
    PJ_XYZ xyz;
    // We need to add lam0 (+lon_0) since it is subtracted in fwd_prepare(),
    // which is desirable for map projections, but not
    // for that method which modifies only the Z component.
    xyz.x = lpz.lam + P->lam0;
    xyz.y = lpz.phi;
    xyz.z = lpz.z + get_forward_offset(P, lpz.phi, lpz.lam);
    return xyz;
}

static PJ_LPZ reverse_3d(PJ_XYZ xyz, PJ *P) {
    PJ_LPZ lpz;
    // We need to subtract lam0 (+lon_0) since it is added in inv_finalize(),
    // which is desirable for map projections, but not
    // for that method which modifies only the Z component.
    lpz.lam = xyz.x - P->lam0;
    lpz.phi = xyz.y;
    lpz.z = xyz.z - get_forward_offset(P, lpz.phi, lpz.lam);
    return lpz;
}

/* Arcsecond to radians */
#define ARCSEC_TO_RAD (DEG_TO_RAD / 3600.0)

PJ *PJ_TRANSFORMATION(vertoffset, 1) {
    struct pj_opaque_vertoffset *Q = static_cast<struct pj_opaque_vertoffset *>(
        calloc(1, sizeof(struct pj_opaque_vertoffset)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = (void *)Q;

    P->fwd3d = forward_3d;
    P->inv3d = reverse_3d;

    P->left = PJ_IO_UNITS_RADIANS;
    P->right = PJ_IO_UNITS_RADIANS;

    /* read args */
    Q->slope_lon = pj_param(P->ctx, P->params, "dslope_lon").f * ARCSEC_TO_RAD;
    Q->slope_lat = pj_param(P->ctx, P->params, "dslope_lat").f * ARCSEC_TO_RAD;
    Q->zoff = pj_param(P->ctx, P->params, "ddh").f;
    const double sinlat0 = sin(P->phi0);
    const double oneMinusEsSinlat0Square = 1 - P->es * (sinlat0 * sinlat0);
    Q->rho0 = P->a * (1 - P->es) /
              (oneMinusEsSinlat0Square * sqrt(oneMinusEsSinlat0Square));
    Q->nu0 = P->a / sqrt(oneMinusEsSinlat0Square);

    return P;
}
