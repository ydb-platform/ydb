/******************************************************************************
 * Project:  PROJ
 * Purpose:  Convert between geocentric coordinates and topocentric (ENU)
 *coordinates
 *
 ******************************************************************************
 * Copyright (c) 2020, Even Rouault <even.rouault at spatialys.com>
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

#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(topocentric, "Geocentric/Topocentric conversion");

// Notations and formulas taken from IOGP Publication 373-7-2 -
// Geomatics Guidance Note number 7, part 2 - October 2020

namespace { // anonymous namespace
struct pj_opaque {
    double X0;
    double Y0;
    double Z0;
    double sinphi0;
    double cosphi0;
    double sinlam0;
    double coslam0;
};
} // anonymous namespace

// Convert from geocentric to topocentric
static void topocentric_fwd(PJ_COORD &coo, PJ *P) {
    struct pj_opaque *Q = static_cast<struct pj_opaque *>(P->opaque);
    const double dX = coo.xyz.x - Q->X0;
    const double dY = coo.xyz.y - Q->Y0;
    const double dZ = coo.xyz.z - Q->Z0;
    coo.xyz.x = -dX * Q->sinlam0 + dY * Q->coslam0;
    coo.xyz.y = -dX * Q->sinphi0 * Q->coslam0 - dY * Q->sinphi0 * Q->sinlam0 +
                dZ * Q->cosphi0;
    coo.xyz.z = dX * Q->cosphi0 * Q->coslam0 + dY * Q->cosphi0 * Q->sinlam0 +
                dZ * Q->sinphi0;
}

// Convert from topocentric to geocentric
static void topocentric_inv(PJ_COORD &coo, PJ *P) {
    struct pj_opaque *Q = static_cast<struct pj_opaque *>(P->opaque);
    const double x = coo.xyz.x;
    const double y = coo.xyz.y;
    const double z = coo.xyz.z;
    coo.xyz.x = Q->X0 - x * Q->sinlam0 - y * Q->sinphi0 * Q->coslam0 +
                z * Q->cosphi0 * Q->coslam0;
    coo.xyz.y = Q->Y0 + x * Q->coslam0 - y * Q->sinphi0 * Q->sinlam0 +
                z * Q->cosphi0 * Q->sinlam0;
    coo.xyz.z = Q->Z0 + y * Q->cosphi0 + z * Q->sinphi0;
}

/*********************************************************************/
PJ *PJ_CONVERSION(topocentric, 1) {
    /*********************************************************************/
    struct pj_opaque *Q =
        static_cast<struct pj_opaque *>(calloc(1, sizeof(struct pj_opaque)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = static_cast<void *>(Q);

    // The topocentric origin can be specified either in geocentric coordinates
    // (X_0,Y_0,Z_0) or as geographic coordinates (lon_0,lat_0,h_0)
    // Checks:
    // - X_0 or lon_0 must be specified
    // - If X_0 is specified, the Y_0 and Z_0 must also be
    // - If lon_0 is specified, then lat_0 must also be
    // - If any of X_0, Y_0, Z_0 is specified, then any of lon_0,lat_0,h_0 must
    //   not be, and vice versa.
    const auto hasX0 = pj_param_exists(P->params, "X_0");
    const auto hasY0 = pj_param_exists(P->params, "Y_0");
    const auto hasZ0 = pj_param_exists(P->params, "Z_0");
    const auto hasLon0 = pj_param_exists(P->params, "lon_0");
    const auto hasLat0 = pj_param_exists(P->params, "lat_0");
    const auto hash0 = pj_param_exists(P->params, "h_0");
    if (!hasX0 && !hasLon0) {
        proj_log_error(P, _("missing X_0 or lon_0"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    if ((hasX0 || hasY0 || hasZ0) && (hasLon0 || hasLat0 || hash0)) {
        proj_log_error(
            P, _("(X_0,Y_0,Z_0) and (lon_0,lat_0,h_0) are mutually exclusive"));
        return pj_default_destructor(
            P, PROJ_ERR_INVALID_OP_MUTUALLY_EXCLUSIVE_ARGS);
    }
    if (hasX0 && (!hasY0 || !hasZ0)) {
        proj_log_error(P, _("missing Y_0 and/or Z_0"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    if (hasLon0 && !hasLat0) // allow missing h_0
    {
        proj_log_error(P, _("missing lat_0"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    // Pass a dummy ellipsoid definition that will be overridden just afterwards
    PJ *cart = proj_create(P->ctx, "+proj=cart +a=1");
    if (cart == nullptr)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    /* inherit ellipsoid definition from P to cart */
    pj_inherit_ellipsoid_def(P, cart);

    if (hasX0) {
        Q->X0 = pj_param(P->ctx, P->params, "dX_0").f;
        Q->Y0 = pj_param(P->ctx, P->params, "dY_0").f;
        Q->Z0 = pj_param(P->ctx, P->params, "dZ_0").f;

        // Compute lam0, phi0 from X0,Y0,Z0
        PJ_XYZ xyz;
        xyz.x = Q->X0;
        xyz.y = Q->Y0;
        xyz.z = Q->Z0;
        const auto lpz = pj_inv3d(xyz, cart);
        Q->sinphi0 = sin(lpz.phi);
        Q->cosphi0 = cos(lpz.phi);
        Q->sinlam0 = sin(lpz.lam);
        Q->coslam0 = cos(lpz.lam);
    } else {
        // Compute X0,Y0,Z0 from lam0, phi0, h0
        PJ_LPZ lpz;
        lpz.lam = P->lam0;
        lpz.phi = P->phi0;
        lpz.z = pj_param(P->ctx, P->params, "dh_0").f;
        const auto xyz = pj_fwd3d(lpz, cart);
        Q->X0 = xyz.x;
        Q->Y0 = xyz.y;
        Q->Z0 = xyz.z;

        Q->sinphi0 = sin(P->phi0);
        Q->cosphi0 = cos(P->phi0);
        Q->sinlam0 = sin(P->lam0);
        Q->coslam0 = cos(P->lam0);
    }

    proj_destroy(cart);

    P->fwd4d = topocentric_fwd;
    P->inv4d = topocentric_inv;
    P->left = PJ_IO_UNITS_CARTESIAN;
    P->right = PJ_IO_UNITS_CARTESIAN;
    return P;
}
