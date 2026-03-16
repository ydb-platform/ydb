/******************************************************************************
 * Project:  PROJ
 * Purpose:  Implementing the S2 family of projections in PROJ
 * Author:   Marcus Elia, <marcus at geopi.pe>
 *
 ******************************************************************************
 * Copyright (c) 2021, Marcus Elia, <marcus at geopi.pe>
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
 *****************************************************************************
 *
 * This implements the S2 projection.  This code is similar
 * to the QSC projection.
 *
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
 * You must also choose which conversion from UV to ST coordinates
 * is used (linear, quadratic, tangent). Read about them in
 * https://github.com/google/s2geometry/blob/0c4c460bdfe696da303641771f9def900b3e440f/src/s2/s2coords.h
 * The S2 projection functions are adapted from the above link and the S2
 * Math util functions are adapted from
 * https://github.com/google/s2geometry/blob/0c4c460bdfe696da303641771f9def900b3e440f/src/s2/util/math/vector.h
 ****************************************************************************/

/* enable predefined math constants M_* for MS Visual Studio */
#if defined(_MSC_VER) || defined(_WIN32)
#ifndef _USE_MATH_DEFINES
#define _USE_MATH_DEFINES
#endif
#endif

#include <cmath>
#include <cstdint>
#include <errno.h>

#include "proj.h"
#include "proj_internal.h"

/* The six cube faces. */
namespace { // anonymous namespace
enum Face {
    FACE_FRONT = 0,
    FACE_RIGHT = 1,
    FACE_TOP = 2,
    FACE_BACK = 3,
    FACE_LEFT = 4,
    FACE_BOTTOM = 5
};
} // anonymous namespace

enum S2ProjectionType { Linear, Quadratic, Tangent, NoUVtoST };
static std::map<std::string, S2ProjectionType> stringToS2ProjectionType{
    {"linear", Linear},
    {"quadratic", Quadratic},
    {"tangent", Tangent},
    {"none", NoUVtoST}};

namespace { // anonymous namespace
struct pj_s2 {
    enum Face face;
    double a_squared;
    double one_minus_f;
    double one_minus_f_squared;
    S2ProjectionType UVtoST;
};
} // anonymous namespace
PROJ_HEAD(s2, "S2") "\n\tMisc, Sph&Ell";

/* The four areas on a cube face. AREA_0 is the area of definition,
 * the other three areas are counted counterclockwise. */
namespace { // anonymous namespace
enum Area { AREA_0 = 0, AREA_1 = 1, AREA_2 = 2, AREA_3 = 3 };
} // anonymous namespace

// =================================================
//
//                  S2 Math Util
//
// =================================================
static PJ_XYZ Abs(const PJ_XYZ &p) {
    return {std::fabs(p.x), std::fabs(p.y), std::fabs(p.z)};
}
// return the index of the largest component (fabs)
static int LargestAbsComponent(const PJ_XYZ &p) {
    PJ_XYZ temp = Abs(p);
    return temp.x > temp.y ? temp.x > temp.z ? 0 : 2 : temp.y > temp.z ? 1 : 2;
}

// =================================================
//
//              S2 Projection Functions
//
// =================================================

// Unfortunately, tan(M_PI_4) is slightly less than 1.0.  This isn't due to
// a flaw in the implementation of tan(), it's because the derivative of
// tan(x) at x=pi/4 is 2, and it happens that the two adjacent floating
// point numbers on either side of the infinite-precision value of pi/4 have
// tangents that are slightly below and slightly above 1.0 when rounded to
// the nearest double-precision result.
static double STtoUV(double s, S2ProjectionType s2_projection) {
    switch (s2_projection) {
    case Linear:
        return 2 * s - 1;
        break;
    case Quadratic:
        if (s >= 0.5)
            return (1 / 3.) * (4 * s * s - 1);
        else
            return (1 / 3.) * (1 - 4 * (1 - s) * (1 - s));
        break;
    case Tangent:
        s = std::tan(M_PI_2 * s - M_PI_4);
        return s +
               (1.0 / static_cast<double>(static_cast<std::int64_t>(1) << 53)) *
                   s;
        break;
    default:
        return s;
    }
}

static double UVtoST(double u, S2ProjectionType s2_projection) {
    double ret = u;
    switch (s2_projection) {
    case Linear:
        ret = 0.5 * (u + 1);
        break;
    case Quadratic:
        if (u >= 0)
            ret = 0.5 * std::sqrt(1 + 3 * u);
        else
            ret = 1 - 0.5 * std::sqrt(1 - 3 * u);
        break;
    case Tangent: {
        volatile double a = std::atan(u);
        ret = (2 * M_1_PI) * (a + M_PI_4);
        break;
    }
    case NoUVtoST:
        break;
    }
    return ret;
}

inline PJ_XYZ FaceUVtoXYZ(int face, double u, double v) {
    switch (face) {
    case 0:
        return {1, u, v};
    case 1:
        return {-u, 1, v};
    case 2:
        return {-u, -v, 1};
    case 3:
        return {-1, -v, -u};
    case 4:
        return {v, -1, -u};
    default:
        return {v, u, -1};
    }
}

inline PJ_XYZ FaceUVtoXYZ(int face, const PJ_XY &uv) {
    return FaceUVtoXYZ(face, uv.x, uv.y);
}

inline void ValidFaceXYZtoUV(int face, const PJ_XYZ &p, double *pu,
                             double *pv) {
    switch (face) {
    case 0:
        *pu = p.y / p.x;
        *pv = p.z / p.x;
        break;
    case 1:
        *pu = -p.x / p.y;
        *pv = p.z / p.y;
        break;
    case 2:
        *pu = -p.x / p.z;
        *pv = -p.y / p.z;
        break;
    case 3:
        *pu = p.z / p.x;
        *pv = p.y / p.x;
        break;
    case 4:
        *pu = p.z / p.y;
        *pv = -p.x / p.y;
        break;
    default:
        *pu = -p.y / p.z;
        *pv = -p.x / p.z;
        break;
    }
}

inline void ValidFaceXYZtoUV(int face, const PJ_XYZ &p, PJ_XY *puv) {
    ValidFaceXYZtoUV(face, p, &(*puv).x, &(*puv).y);
}

inline int GetFace(const PJ_XYZ &p) {
    int face = LargestAbsComponent(p);
    double pFace;
    switch (face) {
    case 0:
        pFace = p.x;
        break;
    case 1:
        pFace = p.y;
        break;
    default:
        pFace = p.z;
        break;
    }
    if (pFace < 0)
        face += 3;
    return face;
}

inline int XYZtoFaceUV(const PJ_XYZ &p, double *pu, double *pv) {
    int face = GetFace(p);
    ValidFaceXYZtoUV(face, p, pu, pv);
    return face;
}

inline int XYZtoFaceUV(const PJ_XYZ &p, PJ_XY *puv) {
    return XYZtoFaceUV(p, &(*puv).x, &(*puv).y);
}

inline bool FaceXYZtoUV(int face, const PJ_XYZ &p, double *pu, double *pv) {
    double pFace;
    switch (face) {
    case 0:
        pFace = p.x;
        break;
    case 1:
        pFace = p.y;
        break;
    case 2:
        pFace = p.z;
        break;
    case 3:
        pFace = p.x;
        break;
    case 4:
        pFace = p.y;
        break;
    default:
        pFace = p.z;
        break;
    }
    if (face < 3) {
        if (pFace <= 0)
            return false;
    } else {
        if (pFace >= 0)
            return false;
    }
    ValidFaceXYZtoUV(face, p, pu, pv);
    return true;
}

inline bool FaceXYZtoUV(int face, const PJ_XYZ &p, PJ_XY *puv) {
    return FaceXYZtoUV(face, p, &(*puv).x, &(*puv).y);
}

// This function inverts ValidFaceXYZtoUV()
inline bool UVtoSphereXYZ(int face, double u, double v, PJ_XYZ *xyz) {
    double major_coord = 1 / sqrt(1 + u * u + v * v);
    double minor_coord_1 = u * major_coord;
    double minor_coord_2 = v * major_coord;

    switch (face) {
    case 0:
        xyz->x = major_coord;
        xyz->y = minor_coord_1;
        xyz->z = minor_coord_2;
        break;
    case 1:
        xyz->x = -minor_coord_1;
        xyz->y = major_coord;
        xyz->z = minor_coord_2;
        break;
    case 2:
        xyz->x = -minor_coord_1;
        xyz->y = -minor_coord_2;
        xyz->z = major_coord;
        break;
    case 3:
        xyz->x = -major_coord;
        xyz->y = -minor_coord_2;
        xyz->z = -minor_coord_1;
        break;
    case 4:
        xyz->x = minor_coord_2;
        xyz->y = -major_coord;
        xyz->z = -minor_coord_1;
        break;
    default:
        xyz->x = minor_coord_2;
        xyz->y = minor_coord_1;
        xyz->z = -major_coord;
        break;
    }

    return true;
}

// ============================================
//
//      The Forward and Inverse Functions
//
// ============================================
static PJ_XY s2_forward(PJ_LP lp, PJ *P) {
    struct pj_s2 *Q = static_cast<struct pj_s2 *>(P->opaque);
    double lat;

    /* Convert the geodetic latitude to a geocentric latitude.
     * This corresponds to the shift from the ellipsoid to the sphere
     * described in [LK12]. */
    if (P->es != 0.0) {
        lat = atan(Q->one_minus_f_squared * tan(lp.phi));
    } else {
        lat = lp.phi;
    }

    // Convert the lat/long to x,y,z on the unit sphere
    double x, y, z;
    double sinlat, coslat;
    double sinlon, coslon;

    sinlat = sin(lat);
    coslat = cos(lat);
    sinlon = sin(lp.lam);
    coslon = cos(lp.lam);
    x = coslat * coslon;
    y = coslat * sinlon;
    z = sinlat;

    PJ_XYZ spherePoint{x, y, z};
    PJ_XY uvCoords;

    ValidFaceXYZtoUV(Q->face, spherePoint, &uvCoords.x, &uvCoords.y);
    double s = UVtoST(uvCoords.x, Q->UVtoST);
    double t = UVtoST(uvCoords.y, Q->UVtoST);
    return {s, t};
}

static PJ_LP s2_inverse(PJ_XY xy, PJ *P) {
    PJ_LP lp = {0.0, 0.0};
    struct pj_s2 *Q = static_cast<struct pj_s2 *>(P->opaque);

    // Do the S2 projections to get from s,t to u,v to x,y,z
    double u = STtoUV(xy.x, Q->UVtoST);
    double v = STtoUV(xy.y, Q->UVtoST);

    PJ_XYZ sphereCoords;
    UVtoSphereXYZ(Q->face, u, v, &sphereCoords);
    double q = sphereCoords.x;
    double r = sphereCoords.y;
    double s = sphereCoords.z;

    // Get the spherical angles from the x y z
    lp.phi = acos(-s) - M_HALFPI;
    lp.lam = atan2(r, q);

    /* Apply the shift from the sphere to the ellipsoid as described
     * in [LK12]. */
    if (P->es != 0.0) {
        int invert_sign;
        volatile double tanphi, xa;
        invert_sign = (lp.phi < 0.0 ? 1 : 0);
        tanphi = tan(lp.phi);
        xa = P->b / sqrt(tanphi * tanphi + Q->one_minus_f_squared);
        lp.phi = atan(sqrt(Q->a_squared - xa * xa) / (Q->one_minus_f * xa));
        if (invert_sign) {
            lp.phi = -lp.phi;
        }
    }

    return lp;
}

PJ *PJ_PROJECTION(s2) {
    struct pj_s2 *Q =
        static_cast<struct pj_s2 *>(calloc(1, sizeof(struct pj_s2)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    /* Determine which UVtoST function is to be used */
    PROJVALUE maybeUVtoST = pj_param(P->ctx, P->params, "sUVtoST");
    if (nullptr != maybeUVtoST.s) {
        try {
            Q->UVtoST = stringToS2ProjectionType.at(maybeUVtoST.s);
        } catch (const std::out_of_range &) {
            proj_log_error(
                P, _("Invalid value for s2 parameter: should be linear, "
                     "quadratic, tangent, or none."));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    } else {
        Q->UVtoST = Quadratic;
    }

    P->left = PJ_IO_UNITS_RADIANS;
    P->right = PJ_IO_UNITS_PROJECTED;
    P->from_greenwich = -P->lam0;

    P->inv = s2_inverse;
    P->fwd = s2_forward;

    /* Determine the cube face from the center of projection. */
    if (P->phi0 >= M_HALFPI - M_FORTPI / 2.0) {
        Q->face = FACE_TOP;
    } else if (P->phi0 <= -(M_HALFPI - M_FORTPI / 2.0)) {
        Q->face = FACE_BOTTOM;
    } else if (fabs(P->lam0) <= M_FORTPI) {
        Q->face = FACE_FRONT;
    } else if (fabs(P->lam0) <= M_HALFPI + M_FORTPI) {
        Q->face = (P->lam0 > 0.0 ? FACE_RIGHT : FACE_LEFT);
    } else {
        Q->face = FACE_BACK;
    }
    /* Fill in useful values for the ellipsoid <-> sphere shift
     * described in [LK12]. */
    if (P->es != 0.0) {
        Q->a_squared = P->a * P->a;
        Q->one_minus_f = 1.0 - (P->a - P->b) / P->a;
        Q->one_minus_f_squared = Q->one_minus_f * Q->one_minus_f;
    }

    return P;
}
