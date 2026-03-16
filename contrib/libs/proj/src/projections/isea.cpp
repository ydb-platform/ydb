/*
   The public domain code for the forward direction was initially
   written by Nathan Wagner.

   The inverse projection was adapted from Java and eC by
   Jérôme Jacovella-St-Louis, originally from the Franz-Benjamin Mocnik's ISEA
   implementation found at
   https://github.com/mocnik-science/geogrid/blob/master/
    src/main/java/org/giscience/utils/geogrid/projections/ISEAProjection.java
   with the following license:
   --------------------------------------------------------------------------
   MIT License

   Copyright (c) 2017-2019 Heidelberg University

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in
   all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

/* The ISEA projection a projects a sphere on the icosahedron. Thereby the size
 * of areas mapped to the icosahedron are preserved. Angles and distances are
 * however slightly distorted. The angular distortion is below 17.27 degree, and
 * the scale variation is less than 16.3 per cent.
 *
 * The projection has been proposed and has been described in detail by:
 *
 * John P. Snyder: An equal-area map projection for polyhedral globes.
 * Cartographica, 29(1), 10–21, 1992. doi:10.3138/27H7-8K88-4882-1752
 *
 * Another description and improvements can be found in:
 *
 * Erika Harrison, Ali Mahdavi-Amiri, and Faramarz Samavati: Optimization of
 * inverse Snyder polyhedral projection. International Conference on Cyberworlds
 * 2011. doi:10.1109/CW.2011.36
 *
 * Erika Harrison, Ali Mahdavi-Amiri, and Faramarz Samavati: Analysis of inverse
 * Snyder optimizations. In: Marina L. Gavrilova, and C. J. Kenneth Tan (Eds):
 * Transactions on Computational Science XVI. Heidelberg, Springer, 2012. pp.
 * 134–148. doi:10.1007/978-3-642-32663-9_8
 */

#include <errno.h>
#include <float.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <limits>

#include "proj.h"
#include "proj_internal.h"
#include <math.h>

#define DEG36 0.62831853071795864768
#define DEG72 1.25663706143591729537
#define DEG90 M_PI_2
#define DEG108 1.88495559215387594306
#define DEG120 2.09439510239319549229
#define DEG144 2.51327412287183459075
#define DEG180 M_PI

/* sqrt(5)/M_PI */
#define ISEA_SCALE 0.8301572857837594396028083

/* 26.565051177 degrees */
#define V_LAT 0.46364760899944494524

// Latitude of center of top icosahedron faces
// atan((3 + sqrt(5)) / 4) = 52.6226318593487 degrees
#define E_RAD 0.91843818701052843323

// Latitude of center of faces mirroring top icosahedron face
// atan((3 - sqrt(5)) / 4) = 10.8123169635739 degrees
#define F_RAD 0.18871053078356206978

// #define phi ((1 + sqrt(5)) / 2)
// #define atanphi 1.01722196789785136772

// g: Spherical distance from center of polygon face to
//    any of its vertices on the sphere
// g = F + 2 * atan(phi) - 90 deg -- sdc2vos
#define sdc2vos 0.6523581397843681859886783

#define tang 0.76393202250021030358019673567 // tan(sdc2vos)

// theta (30 degrees) is plane angle between radius
// vector to center and adjacent edge of plane polygon

#define tan30 0.57735026918962576450914878 // tan(DEG_TO_RAD * 30)
#define cotTheta (1.0 / tan30)

// G: spherical angle between radius vector to center and adjacent edge
//    of spherical polygon on the globe (36 degrees)
//    cos(DEG_TO_RAD * 36)
#define cosG 0.80901699437494742410229341718281905886
//    sin(DEG_TO_RAD * 36)
#define sinG 0.587785252292473129168705954639072768597652
//    cos(g)
#define cosSDC2VoS 0.7946544722917661229596057297879189448539

#define sinGcosSDC2VoS (sinG * cosSDC2VoS) // sin G cos g

#define SQRT3 1.73205080756887729352744634150587236694280525381038
#define sin60 (SQRT3 / 2.0)
#define cos30 (SQRT3 / 2.0)

// tang * sin(60 deg)
#define TABLE_G (tang * sin60)

// (1 / (2 * sqrt(5)) + 1 / 6.0) * sqrt(M_PI * sqrt(3))
#define RprimeOverR 0.9103832815095032 // R' / R

/* H = 0.25 R tan g = */
#define TABLE_H (0.25 * tang)

/* in radians */
#define ISEA_STD_LAT 1.01722196792335072101
#define ISEA_STD_LONG .19634954084936207740

namespace { // anonymous namespace

struct GeoPoint {
    double lat, lon;
}; // In radians

struct hex {
    int iso;
    long x, y, z;
};
} // anonymous namespace

/* y *must* be positive down as the xy /iso conversion assumes this */
static void hex_xy(struct hex *h) {
    if (!h->iso)
        return;
    if (h->x >= 0) {
        h->y = -h->y - (h->x + 1) / 2;
    } else {
        /* need to round toward -inf, not toward zero, so x-1 */
        h->y = -h->y - h->x / 2;
    }
    h->iso = 0;
}

static void hex_iso(struct hex *h) {
    if (h->iso)
        return;

    if (h->x >= 0) {
        h->y = (-h->y - (h->x + 1) / 2);
    } else {
        /* need to round toward -inf, not toward zero, so x-1 */
        h->y = (-h->y - (h->x) / 2);
    }

    h->z = -h->x - h->y;
    h->iso = 1;
}

static void hexbin2(double width, double x, double y, long *i, long *j) {
    double z, rx, ry, rz;
    double abs_dx, abs_dy, abs_dz;
    long ix, iy, iz, s;
    struct hex h;

    x = x / cos(30 * M_PI / 180.0); /* rotated X coord */
    y = y - x / 2.0;                /* adjustment for rotated X */

    /* adjust for actual hexwidth */
    if (width == 0) {
        throw "Division by zero";
    }
    x /= width;
    y /= width;

    z = -x - y;

    rx = floor(x + 0.5);
    ix = lround(rx);
    ry = floor(y + 0.5);
    iy = lround(ry);
    rz = floor(z + 0.5);
    iz = lround(rz);
    if (fabs((double)ix + iy) > std::numeric_limits<int>::max() ||
        fabs((double)ix + iy + iz) > std::numeric_limits<int>::max()) {
        throw "Integer overflow";
    }

    s = ix + iy + iz;

    if (s) {
        abs_dx = fabs(rx - x);
        abs_dy = fabs(ry - y);
        abs_dz = fabs(rz - z);

        if (abs_dx >= abs_dy && abs_dx >= abs_dz) {
            ix -= s;
        } else if (abs_dy >= abs_dx && abs_dy >= abs_dz) {
            iy -= s;
        } else {
            iz -= s;
        }
    }
    h.x = ix;
    h.y = iy;
    h.z = iz;
    h.iso = 1;

    hex_xy(&h);
    *i = h.x;
    *j = h.y;
}

#define numIcosahedronFaces 20

namespace { // anonymous namespace
enum isea_address_form { ISEA_PLANE, ISEA_Q2DI, ISEA_Q2DD, ISEA_HEX };

struct isea_sincos {
    double s, c;
};

struct isea_pt {
    double x, y;
};

} // anonymous namespace

// distortion
// static double maximumAngularDistortion = 17.27;
// static double maximumScaleVariation = 1.163;
// static double minimumScaleVariation = .860;

// Vertices of dodecahedron centered in icosahedron triangular faces
static const GeoPoint facesCenterDodecahedronVertices[numIcosahedronFaces] = {
    {E_RAD, DEG_TO_RAD * -144},  {E_RAD, DEG_TO_RAD * -72},
    {E_RAD, DEG_TO_RAD * 0},     {E_RAD, DEG_TO_RAD * 72},
    {E_RAD, DEG_TO_RAD * 144},   {F_RAD, DEG_TO_RAD * -144},
    {F_RAD, DEG_TO_RAD * -72},   {F_RAD, DEG_TO_RAD * 0},
    {F_RAD, DEG_TO_RAD * 72},    {F_RAD, DEG_TO_RAD * 144},
    {-F_RAD, DEG_TO_RAD * -108}, {-F_RAD, DEG_TO_RAD * -36},
    {-F_RAD, DEG_TO_RAD * 36},   {-F_RAD, DEG_TO_RAD * 108},
    {-F_RAD, DEG_TO_RAD * 180},  {-E_RAD, DEG_TO_RAD * -108},
    {-E_RAD, DEG_TO_RAD * -36},  {-E_RAD, DEG_TO_RAD * 36},
    {-E_RAD, DEG_TO_RAD * 108},  {-E_RAD, DEG_TO_RAD * 180}};

// NOTE: Very similar to ISEAPlanarProjection::faceOrientation(),
//       but the forward projection sometimes is returning a negative M_PI
static inline double az_adjustment(int triangle) {
    if ((triangle >= 5 && triangle <= 9) || triangle == 15 || triangle == 16)
        return M_PI;
    else if (triangle >= 17)
        return -M_PI;
    return 0;
}

static struct isea_pt isea_triangle_xy(int triangle) {
    struct isea_pt c;

    triangle %= numIcosahedronFaces;

    c.x = TABLE_G * ((triangle % 5) - 2) * 2.0;
    if (triangle > 9) {
        c.x += TABLE_G;
    }

    // REVIEW: This is likely related to
    //         pj_isea_data::yOffsets
    switch (triangle / 5) {
    case 0:
        c.y = 5.0 * TABLE_H;
        break;
    case 1:
        c.y = TABLE_H;
        break;
    case 2:
        c.y = -TABLE_H;
        break;
    case 3:
        c.y = -5.0 * TABLE_H;
        break;
    default:
        /* should be impossible */
        exit(EXIT_FAILURE);
    }
    c.x *= RprimeOverR;
    c.y *= RprimeOverR;

    return c;
}

namespace { // anonymous namespace

class ISEAPlanarProjection;

struct pj_isea_data {
    double o_lat, o_lon, o_az; /* orientation, radians */
    int aperture;              /* valid values depend on partitioning method */
    int resolution;
    isea_address_form output; /* an isea_address_form */
    int triangle;             /* triangle of last transformed point */
    int quad;                 /* quad of last transformed point */
    isea_sincos vertexLatSinCos[numIcosahedronFaces];

    double R2;
    double Rprime;
    double Rprime2X;
    double RprimeTang;
    double Rprime2Tan2g;
    double triTang;
    double centerToBase;
    double triWidth;
    double yOffsets[4];
    double xo, yo;
    double sx, sy;
    ISEAPlanarProjection *p;

    void initialize(const PJ *P);
};
} // anonymous namespace

#ifdef _MSC_VER
#pragma warning(push)
/* disable unreachable code warning for return 0 */
#pragma warning(disable : 4702)
#endif

#define SAFE_ARC_EPSILON 1E-15

static inline double safeArcSin(double t) {
    return fabs(t) < SAFE_ARC_EPSILON         ? 0
           : fabs(t - 1.0) < SAFE_ARC_EPSILON ? M_PI / 2
           : fabs(t + 1.0) < SAFE_ARC_EPSILON ? -M_PI / 2
                                              : asin(t);
}

static inline double safeArcCos(double t) {
    return fabs(t) < SAFE_ARC_EPSILON       ? M_PI / 2
           : fabs(t + 1) < SAFE_ARC_EPSILON ? M_PI
           : fabs(t - 1) < SAFE_ARC_EPSILON ? 0
                                            : acos(t);
}

#undef SAFE_ARC_EPSILON

/* coord needs to be in radians */
static int isea_snyder_forward(const struct pj_isea_data *data,
                               const struct GeoPoint *ll, struct isea_pt *out) {
    int i;
    double sinLat = sin(ll->lat), cosLat = cos(ll->lat);

    /*
     * TODO by locality of reference, start by trying the same triangle
     * as last time
     */
    for (i = 0; i < numIcosahedronFaces; i++) {
        /* additional variables from snyder */
        double q, H, Ag, Azprime, Az, dprime, f, rho, x, y;

        /* variables used to store intermediate results */
        double az_offset;

        /* how many multiples of 60 degrees we adjust the azimuth */
        int Az_adjust_multiples;

        const struct GeoPoint *center = &facesCenterDodecahedronVertices[i];
        const struct isea_sincos *centerLatSinCos = &data->vertexLatSinCos[i];
        double dLon = ll->lon - center->lon;
        double cosLat_cosLon = cosLat * cos(dLon);
        double cosZ =
            centerLatSinCos->s * sinLat + centerLatSinCos->c * cosLat_cosLon;
        double sinAz, cosAz;

        /* step 1 */
        double z = safeArcCos(cosZ);

        /* not on this triangle */
        if (z > sdc2vos /*g*/ + 0.000005) { /* TODO DBL_EPSILON */
            continue;
        }

        /* snyder eq 14 */
        Az = atan2(cosLat * sin(dLon), centerLatSinCos->c * sinLat -
                                           centerLatSinCos->s * cosLat_cosLon);

        /* step 2 */

        /* This calculates "some" vertex coordinate */
        az_offset = az_adjustment(i);

        Az -= az_offset;

        /* TODO I don't know why we do this.  It's not in snyder */
        /* maybe because we should have picked a better vertex */
        if (Az < 0.0) {
            Az += 2.0 * M_PI;
        }
        /*
         * adjust Az for the point to fall within the range of 0 to
         * 2(90 - theta) or 60 degrees for the hexagon, by
         * and therefore 120 degrees for the triangle
         * of the icosahedron
         * subtracting or adding multiples of 60 degrees to Az and
         * recording the amount of adjustment
         */

        Az_adjust_multiples = 0;
        while (Az < 0.0) {
            Az += DEG120;
            Az_adjust_multiples--;
        }
        while (Az > DEG120 + DBL_EPSILON) {
            Az -= DEG120;
            Az_adjust_multiples++;
        }

        /* step 3 */

        /* Calculate q from eq 9. */
        cosAz = cos(Az);
        sinAz = sin(Az);
        q = atan2(tang, cosAz + sinAz * cotTheta);

        /* not in this triangle */
        if (z > q + 0.000005) {
            continue;
        }
        /* step 4 */

        /* Apply equations 5-8 and 10-12 in order */

        /* eq 5 */
        /* R' in the paper is for the truncated (icosahedron?) */

        /* eq 6 */
        H = acos(sinAz * sinGcosSDC2VoS /* sin(G) * cos(g) */ - cosAz * cosG);

        /* eq 7 */
        /* Ag = (Az + G + H - DEG180) * M_PI * R * R / DEG180; */
        Ag = Az + DEG_TO_RAD * 36 /* G */ + H - DEG180;

        /* eq 8 */
        Azprime = atan2(2.0 * Ag, RprimeOverR * RprimeOverR * tang * tang -
                                      2.0 * Ag * cotTheta);

        /* eq 10 */
        /* cot(theta) = 1.73205080756887729355 */
        dprime = RprimeOverR * tang / (cos(Azprime) + sin(Azprime) * cotTheta);

        /* eq 11 */
        f = dprime / (2.0 * RprimeOverR * sin(q / 2.0));

        /* eq 12 */
        rho = 2.0 * RprimeOverR * f * sin(z / 2.0);

        /*
         * add back the same 60 degree multiple adjustment from step
         * 2 to Azprime
         */

        Azprime += DEG120 * Az_adjust_multiples;

        /* calculate rectangular coordinates */

        x = rho * sin(Azprime);
        y = rho * cos(Azprime);

        /*
         * TODO
         * translate coordinates to the origin for the particular
         * hexagon on the flattened polyhedral map plot
         */

        out->x = x;
        out->y = y;

        return i;
    }

    /*
     * should be impossible, this implies that the coordinate is not on
     * any triangle
     */

    fprintf(stderr, "impossible transform: %f %f is not on any triangle\n",
            PJ_TODEG(ll->lon), PJ_TODEG(ll->lat));

    exit(EXIT_FAILURE);
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

/*
 * return the new coordinates of any point in original coordinate system.
 * Define a point (newNPold) in original coordinate system as the North Pole in
 * new coordinate system, and the great circle connect the original and new
 * North Pole as the lon0 longitude in new coordinate system, given any point
 * in original coordinate system, this function return the new coordinates.
 */

/* formula from Snyder, Map Projections: A working manual, p31 */
/*
 * old north pole at np in new coordinates
 * could be simplified a bit with fewer intermediates
 *
 * TODO take a result pointer
 */
static struct GeoPoint snyder_ctran(const struct GeoPoint &np,
                                    const struct GeoPoint &pt) {
    struct GeoPoint result;
    double phi = pt.lat, lambda = pt.lon;
    double alpha = np.lat, beta = np.lon;
    double dlambda = lambda - beta /* lambda0 */;
    double cos_p = cos(phi), sin_p = sin(phi);
    double cos_a = cos(alpha), sin_a = sin(alpha);
    double cos_dlambda = cos(dlambda), sin_dlambda = sin(dlambda);

    /* mpawm 5-7 */
    double sin_phip = sin_a * sin_p - cos_a * cos_p * cos_dlambda;

    /* mpawm 5-8b */

    /* use the two argument form so we end up in the right quadrant */
    double lp_b = /* lambda prime minus beta */
        atan2(cos_p * sin_dlambda, sin_a * cos_p * cos_dlambda + cos_a * sin_p);
    double lambdap = lp_b + beta;

    /* normalize longitude */
    /* TODO can we just do a modulus ? */
    lambdap = fmod(lambdap, 2 * M_PI);
    while (lambdap > M_PI)
        lambdap -= 2 * M_PI;
    while (lambdap < -M_PI)
        lambdap += 2 * M_PI;

    result.lat = safeArcSin(sin_phip);
    result.lon = lambdap;
    return result;
}

static struct GeoPoint isea_ctran(const struct GeoPoint *np,
                                  const struct GeoPoint *pt, double lon0) {
    struct GeoPoint cnp = {np->lat, np->lon + M_PI};
    struct GeoPoint npt = snyder_ctran(cnp, *pt);

    npt.lon -= (/* M_PI */ -lon0 + np->lon);
    /*
     * snyder is down tri 3, isea is along side of tri1 from vertex 0 to
     * vertex 1 these are 180 degrees apart
     */
    // npt.lon += M_PI;

    /* normalize lon */
    npt.lon = fmod(npt.lon, 2 * M_PI);
    while (npt.lon > M_PI)
        npt.lon -= 2 * M_PI;
    while (npt.lon < -M_PI)
        npt.lon += 2 * M_PI;

    return npt;
}

/* fuller's at 5.2454 west, 2.3009 N, adjacent at 7.46658 deg */

static int isea_grid_init(struct pj_isea_data *g) {
    int i;

    if (!g)
        return 0;

    g->o_lat = ISEA_STD_LAT;
    g->o_lon = ISEA_STD_LONG;
    g->o_az = 0.0;
    g->aperture = 4;
    g->resolution = 6;

    for (i = 0; i < numIcosahedronFaces; i++) {
        const GeoPoint *c = &facesCenterDodecahedronVertices[i];
        g->vertexLatSinCos[i].s = sin(c->lat);
        g->vertexLatSinCos[i].c = cos(c->lat);
    }
    return 1;
}

static void isea_orient_isea(struct pj_isea_data *g) {
    if (!g)
        return;
    g->o_lat = ISEA_STD_LAT;
    g->o_lon = ISEA_STD_LONG;
    g->o_az = 0.0;
}

static void isea_orient_pole(struct pj_isea_data *g) {
    if (!g)
        return;
    g->o_lat = M_PI / 2.0;
    g->o_lon = 0.0;
    g->o_az = 0;
}

static int isea_transform(struct pj_isea_data *g, struct GeoPoint *in,
                          struct isea_pt *out) {
    struct GeoPoint i, pole;
    int tri;

    pole.lat = g->o_lat;
    pole.lon = g->o_lon;

    i = isea_ctran(&pole, in, g->o_az);

    tri = isea_snyder_forward(g, &i, out);
    g->triangle = tri;

    return tri;
}

#define DOWNTRI(tri) ((tri / 5) % 2 == 1)

static void isea_rotate(struct isea_pt *pt, double degrees) {
    double rad;

    double x, y;

    rad = -degrees * M_PI / 180.0;
    while (rad >= 2.0 * M_PI)
        rad -= 2.0 * M_PI;
    while (rad <= -2.0 * M_PI)
        rad += 2.0 * M_PI;

    x = pt->x * cos(rad) + pt->y * sin(rad);
    y = -pt->x * sin(rad) + pt->y * cos(rad);

    pt->x = x;
    pt->y = y;
}

static void isea_tri_plane(int tri, struct isea_pt *pt) {
    struct isea_pt tc; /* center of triangle */

    if (DOWNTRI(tri)) {
        pt->x *= -1;
        pt->y *= -1;
    }
    tc = isea_triangle_xy(tri);
    pt->x += tc.x;
    pt->y += tc.y;
}

/* convert projected triangle coords to quad xy coords, return quad number */
static int isea_ptdd(int tri, struct isea_pt *pt) {
    int downtri, quadz;

    downtri = ((tri / 5) % 2 == 1);
    quadz = (tri % 5) + (tri / 10) * 5 + 1;

    // NOTE: This would always be a 60 degrees rotation if the flip were
    //       already done as in isea_tri_plane()
    isea_rotate(pt, downtri ? 240.0 : 60.0);
    if (downtri) {
        pt->x += 0.5;
        /* pt->y += cos(30.0 * M_PI / 180.0); */
        pt->y += cos30;
    }
    return quadz;
}

static int isea_dddi_ap3odd(struct pj_isea_data *g, int quadz,
                            struct isea_pt *pt, struct isea_pt *di) {
    struct isea_pt v;
    double hexwidth;
    double sidelength; /* in hexes */
    long d, i;
    long maxcoord;
    struct hex h;

    /* This is the number of hexes from apex to base of a triangle */
    sidelength = (pow(2.0, g->resolution) + 1.0) / 2.0;

    /* apex to base is cos(30deg) */
    hexwidth = cos(M_PI / 6.0) / sidelength;

    /* TODO I think sidelength is always x.5, so
     * (int)sidelength * 2 + 1 might be just as good
     */
    maxcoord = lround((sidelength * 2.0));

    v = *pt;
    hexbin2(hexwidth, v.x, v.y, &h.x, &h.y);
    h.iso = 0;
    hex_iso(&h);

    d = h.x - h.z;
    i = h.x + h.y + h.y;

    /*
     * you want to test for max coords for the next quad in the same
     * "row" first to get the case where both are max
     */
    if (quadz <= 5) {
        if (d == 0 && i == maxcoord) {
            /* north pole */
            quadz = 0;
            d = 0;
            i = 0;
        } else if (i == maxcoord) {
            /* upper right in next quad */
            quadz += 1;
            if (quadz == 6)
                quadz = 1;
            i = maxcoord - d;
            d = 0;
        } else if (d == maxcoord) {
            /* lower right in quad to lower right */
            quadz += 5;
            d = 0;
        }
    } else /* if (quadz >= 6) */ {
        if (i == 0 && d == maxcoord) {
            /* south pole */
            quadz = 11;
            d = 0;
            i = 0;
        } else if (d == maxcoord) {
            /* lower right in next quad */
            quadz += 1;
            if (quadz == 11)
                quadz = 6;
            d = maxcoord - i;
            i = 0;
        } else if (i == maxcoord) {
            /* upper right in quad to upper right */
            quadz = (quadz - 4) % 5;
            i = 0;
        }
    }

    di->x = d;
    di->y = i;

    g->quad = quadz;
    return quadz;
}

static int isea_dddi(struct pj_isea_data *g, int quadz, struct isea_pt *pt,
                     struct isea_pt *di) {
    struct isea_pt v;
    double hexwidth;
    long sidelength; /* in hexes */
    struct hex h;

    if (g->aperture == 3 && g->resolution % 2 != 0) {
        return isea_dddi_ap3odd(g, quadz, pt, di);
    }
    /* todo might want to do this as an iterated loop */
    if (g->aperture > 0) {
        double sidelengthDouble = pow(g->aperture, g->resolution / 2.0);
        if (fabs(sidelengthDouble) > std::numeric_limits<int>::max()) {
            throw "Integer overflow";
        }
        sidelength = lround(sidelengthDouble);
    } else {
        sidelength = g->resolution;
    }

    if (sidelength == 0) {
        throw "Division by zero";
    }
    hexwidth = 1.0 / sidelength;

    v = *pt;
    isea_rotate(&v, -30.0);
    hexbin2(hexwidth, v.x, v.y, &h.x, &h.y);
    h.iso = 0;
    hex_iso(&h);

    /* we may actually be on another quad */
    if (quadz <= 5) {
        if (h.x == 0 && h.z == -sidelength) {
            /* north pole */
            quadz = 0;
            h.z = 0;
            h.y = 0;
            h.x = 0;
        } else if (h.z == -sidelength) {
            quadz = quadz + 1;
            if (quadz == 6)
                quadz = 1;
            h.y = sidelength - h.x;
            h.z = h.x - sidelength;
            h.x = 0;
        } else if (h.x == sidelength) {
            quadz += 5;
            h.y = -h.z;
            h.x = 0;
        }
    } else /* if (quadz >= 6) */ {
        if (h.z == 0 && h.x == sidelength) {
            /* south pole */
            quadz = 11;
            h.x = 0;
            h.y = 0;
            h.z = 0;
        } else if (h.x == sidelength) {
            quadz = quadz + 1;
            if (quadz == 11)
                quadz = 6;
            h.x = h.y + sidelength;
            h.y = 0;
            h.z = -h.x;
        } else if (h.y == -sidelength) {
            quadz -= 4;
            h.y = 0;
            h.z = -h.x;
        }
    }
    di->x = h.x;
    di->y = -h.z;

    g->quad = quadz;
    return quadz;
}

static int isea_ptdi(struct pj_isea_data *g, int tri, struct isea_pt *pt,
                     struct isea_pt *di) {
    struct isea_pt v;
    int quadz;

    v = *pt;
    quadz = isea_ptdd(tri, &v);
    quadz = isea_dddi(g, quadz, &v, di);
    return quadz;
}

/* TODO just encode the quad in the d or i coordinate
 * quad is 0-11, which can be four bits.
 * d' = d << 4 + q, d = d' >> 4, q = d' & 0xf
 */
/* convert a q2di to global hex coord */
static int isea_hex(struct pj_isea_data *g, int tri, struct isea_pt *pt,
                    struct isea_pt *hex) {
    struct isea_pt v;
#ifdef FIXME
    long sidelength;
    long d, i, x, y;
#endif
    int quadz;

    quadz = isea_ptdi(g, tri, pt, &v);

    if (v.x < (INT_MIN >> 4) || v.x > (INT_MAX >> 4)) {
        throw "Invalid shift";
    }
    hex->x = ((int)v.x * 16) + quadz;
    hex->y = v.y;

    return 1;
#ifdef FIXME
    d = lround(floor(v.x));
    i = lround(floor(v.y));

    /* Aperture 3 odd resolutions */
    if (g->aperture == 3 && g->resolution % 2 != 0) {
        long offset = lround((pow(3.0, g->resolution - 1) + 0.5));

        d += offset * ((g->quadz - 1) % 5);
        i += offset * ((g->quadz - 1) % 5);

        if (quadz == 0) {
            d = 0;
            i = offset;
        } else if (quadz == 11) {
            d = 2 * offset;
            i = 0;
        } else if (quadz > 5) {
            d += offset;
        }

        x = (2 * d - i) / 3;
        y = (2 * i - d) / 3;

        hex->x = x + offset / 3;
        hex->y = y + 2 * offset / 3;
        return 1;
    }

    /* aperture 3 even resolutions and aperture 4 */
    sidelength = lround((pow(g->aperture, g->resolution / 2.0)));
    if (g->quad == 0) {
        hex->x = 0;
        hex->y = sidelength;
    } else if (g->quad == 11) {
        hex->x = sidelength * 2;
        hex->y = 0;
    } else {
        hex->x = d + sidelength * ((g->quad - 1) % 5);
        if (g->quad > 5)
            hex->x += sidelength;
        hex->y = i + sidelength * ((g->quad - 1) % 5);
    }

    return 1;
#endif
}

static struct isea_pt isea_forward(struct pj_isea_data *g,
                                   struct GeoPoint *in) {
    isea_pt out;
    int tri = isea_transform(g, in, &out);

    if (g->output == ISEA_PLANE)
        isea_tri_plane(tri, &out);
    else {
        isea_pt coord;

        /* convert to isea standard triangle size */
        out.x *= ISEA_SCALE; // / g->radius;
        out.y *= ISEA_SCALE; // / g->radius;
        out.x += 0.5;
        out.y += 2.0 * .14433756729740644112;

        switch (g->output) {
        case ISEA_PLANE:
            /* already handled above -- GCC should not be complaining */
        case ISEA_Q2DD:
            /* Same as above, we just don't print as much */
            g->quad = isea_ptdd(tri, &out);
            break;
        case ISEA_Q2DI:
            g->quad = isea_ptdi(g, tri, &out, &coord);
            return coord;
        case ISEA_HEX:
            isea_hex(g, tri, &out, &coord);
            return coord;
        }
    }
    return out;
}

/*
 * Proj 4 integration code follows
 */

PROJ_HEAD(isea, "Icosahedral Snyder Equal Area") "\n\tSph";

static PJ_XY isea_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_isea_data *Q = static_cast<struct pj_isea_data *>(P->opaque);
    struct isea_pt out;
    struct GeoPoint in;

    //  TODO: Convert geodetic latitude to authalic latitude if not
    //        spherical as in eqearth, healpix, laea, etc.
    in.lat = lp.phi;
    in.lon = lp.lam;

    try {
        out = isea_forward(Q, &in);
    } catch (const char *) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().xy;
    }

    xy.x = out.x;
    xy.y = out.y;

    return xy;
}

static PJ_LP isea_s_inverse(PJ_XY xy, PJ *P);

PJ *PJ_PROJECTION(isea) {
    char *opt;
    struct pj_isea_data *Q = static_cast<struct pj_isea_data *>(
        calloc(1, sizeof(struct pj_isea_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    // NOTE: if a inverse was needed, there is some material at
    // https://brsr.github.io/2021/08/31/snyder-equal-area.html
    P->fwd = isea_s_forward;
    P->inv = isea_s_inverse;
    isea_grid_init(Q);
    Q->output = ISEA_PLANE;

    /*      P->radius = P->a; / * otherwise defaults to 1 */
    /* calling library will scale, I think */

    opt = pj_param(P->ctx, P->params, "sorient").s;
    if (opt) {
        if (!strcmp(opt, "isea")) {
            isea_orient_isea(Q);
        } else if (!strcmp(opt, "pole")) {
            isea_orient_pole(Q);
        } else {
            proj_log_error(
                P,
                _("Invalid value for orient: only isea or pole are supported"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    if (pj_param(P->ctx, P->params, "tazi").i) {
        Q->o_az = pj_param(P->ctx, P->params, "razi").f;
    }

    if (pj_param(P->ctx, P->params, "tlon_0").i) {
        Q->o_lon = pj_param(P->ctx, P->params, "rlon_0").f;
    }

    if (pj_param(P->ctx, P->params, "tlat_0").i) {
        Q->o_lat = pj_param(P->ctx, P->params, "rlat_0").f;
    }

    opt = pj_param(P->ctx, P->params, "smode").s;
    if (opt) {
        if (!strcmp(opt, "plane")) {
            Q->output = ISEA_PLANE;
        } else if (!strcmp(opt, "di")) {
            Q->output = ISEA_Q2DI;
        } else if (!strcmp(opt, "dd")) {
            Q->output = ISEA_Q2DD;
        } else if (!strcmp(opt, "hex")) {
            Q->output = ISEA_HEX;
        } else {
            proj_log_error(P, _("Invalid value for mode: only plane, di, dd or "
                                "hex are supported"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    /* REVIEW: Was this an undocumented +rescale= parameter?
    if (pj_param(P->ctx, P->params, "trescale").i) {
        Q->radius = ISEA_SCALE;
    }
    */

    if (pj_param(P->ctx, P->params, "tresolution").i) {
        Q->resolution = pj_param(P->ctx, P->params, "iresolution").i;
    } else {
        Q->resolution = 4;
    }

    if (pj_param(P->ctx, P->params, "taperture").i) {
        Q->aperture = pj_param(P->ctx, P->params, "iaperture").i;
    } else {
        Q->aperture = 3;
    }

    Q->initialize(P);

    return P;
}

#define Min std::min
#define Max std::max

#define inf std::numeric_limits<double>::infinity()

// static define precision = DEG_TO_RAD * 1e-9;
#define precision (DEG_TO_RAD * 1e-11)
#define precisionPerDefinition (DEG_TO_RAD * 1e-5)

#define AzMax (DEG_TO_RAD * 120)

#define westVertexLon (DEG_TO_RAD * -144)

namespace { // anonymous namespace

struct ISEAFacePoint {
    int face;
    double x, y;
};

class ISEAPlanarProjection {
  public:
    explicit ISEAPlanarProjection(const GeoPoint &value)
        : orientation(value), cosOrientationLat(cos(value.lat)),
          sinOrientationLat(sin(value.lat)) {}

    bool cartesianToGeo(const PJ_XY &inPosition, const pj_isea_data *params,
                        GeoPoint &result) {
        bool r = false;
        static const double epsilon = 1E-11;
        int face = 0;
        PJ_XY position = inPosition;

#define sr -sin60 // sin(-60)
#define cr 0.5    // cos(-60)
        if (position.x < 0 ||
            (position.x < params->triWidth / 2 && position.y < 0 &&
             position.y * cr < position.x * sr))
            position.x += 5 * params->triWidth; // Wrap around
// Rotate and shear to determine face if not stored in position.z
#define shearX (1.0 / SQRT3)
        double yp = -(position.x * sr + position.y * cr);
        double x =
            (position.x * cr - position.y * sr + yp * shearX) * params->sx;
        double y = yp * params->sy;
#undef shearX
#undef sr
#undef cr

        if (x < 0 || (y > x && x < 5 - epsilon))
            x += epsilon;
        else if (x > 5 || (y < x && x > 0 + epsilon))
            x -= epsilon;

        if (y < 0 || (x > y && y < 6 - epsilon))
            y += epsilon;
        else if (y > 6 || (x < y && y > 0 + epsilon))
            y -= epsilon;

        if (x >= 0 && x <= 5 && y >= 0 && y <= 6) {
            int ix = Max(0, Min(4, (int)x));
            int iy = Max(0, Min(5, (int)y));

            if (iy == ix || iy == ix + 1) {
                int rhombus = ix + iy;
                bool top = x - ix > y - iy;
                face = -1;

                switch (rhombus) {
                case 0:
                    face = top ? 0 : 5;
                    break;
                case 2:
                    face = top ? 1 : 6;
                    break;
                case 4:
                    face = top ? 2 : 7;
                    break;
                case 6:
                    face = top ? 3 : 8;
                    break;
                case 8:
                    face = top ? 4 : 9;
                    break;

                case 1:
                    face = top ? 10 : 15;
                    break;
                case 3:
                    face = top ? 11 : 16;
                    break;
                case 5:
                    face = top ? 12 : 17;
                    break;
                case 7:
                    face = top ? 13 : 18;
                    break;
                case 9:
                    face = top ? 14 : 19;
                    break;
                }
                face++;
            }
        }

        if (face) {
            int fy = (face - 1) / 5, fx = (face - 1) - 5 * fy;
            double rx =
                position.x - (2 * fx + fy / 2 + 1) * params->triWidth / 2;
            double ry =
                position.y - (params->yOffsets[fy] + 3 * params->centerToBase);
            GeoPoint dst;

            r = icosahedronToSphere({face - 1, rx, ry}, params, dst);

            if (dst.lon < -M_PI - epsilon)
                dst.lon += 2 * M_PI;
            else if (dst.lon > M_PI + epsilon)
                dst.lon -= 2 * M_PI;

            result = {dst.lat, dst.lon};
        }
        return r;
    }

    // Converts coordinates on the icosahedron to geographic coordinates
    // (inverse projection)
    bool icosahedronToSphere(const ISEAFacePoint &c, const pj_isea_data *params,
                             GeoPoint &r) {
        if (c.face >= 0 && c.face < numIcosahedronFaces) {
            double Az = atan2(c.x, c.y); // Az'
            double rho = sqrt(c.x * c.x + c.y * c.y);
            double AzAdjustment = faceOrientation(c.face);

            Az += AzAdjustment;
            while (Az < 0) {
                AzAdjustment += AzMax;
                Az += AzMax;
            }
            while (Az > AzMax) {
                AzAdjustment -= AzMax;
                Az -= AzMax;
            }
            {
                double sinAz = sin(Az), cosAz = cos(Az);
                double cotAz = cosAz / sinAz;
                double area = params->Rprime2Tan2g /
                              (2 * (cotAz + cotTheta)); // A_G or A_{ABD}
                double deltaAz = 10 * precision;
                double degAreaOverR2Plus180Minus36 =
                    area / params->R2 - westVertexLon;
                double Az_earth = Az;

                while (fabs(deltaAz) > precision) {
                    double sinAzEarth = sin(Az_earth),
                           cosAzEarth = cos(Az_earth);
                    double H =
                        acos(sinAzEarth * sinGcosSDC2VoS - cosAzEarth * cosG);
                    double FAz_earth = degAreaOverR2Plus180Minus36 - H -
                                       Az_earth; // F(Az) or g(Az)
                    double F2Az_earth =
                        (cosAzEarth * sinGcosSDC2VoS + sinAzEarth * cosG) /
                            sin(H) -
                        1;                             // F'(Az) or g'(Az)
                    deltaAz = -FAz_earth / F2Az_earth; // Delta Az^0 or Delta Az
                    Az_earth += deltaAz;
                }
                {
                    double sinAz_earth = sin(Az_earth),
                           cosAz_earth = cos(Az_earth);
                    double q =
                        atan2(tang, (cosAz_earth + sinAz_earth * cotTheta));
                    double d =
                        params->RprimeTang / (cosAz + sinAz * cotTheta); // d'
                    double f = d / (params->Rprime2X * sin(q / 2));      // f
                    double z = 2 * asin(rho / (params->Rprime2X * f));

                    Az_earth -= AzAdjustment;
                    {
                        const isea_sincos *latSinCos =
                            &params->vertexLatSinCos[c.face];
                        double sinLat0 = latSinCos->s, cosLat0 = latSinCos->c;
                        double sinZ = sin(z), cosZ = cos(z);
                        double cosLat0SinZ = cosLat0 * sinZ;
                        double latSin =
                            sinLat0 * cosZ + cosLat0SinZ * cos(Az_earth);
                        double lat = safeArcSin(latSin);
                        double lon =
                            facesCenterDodecahedronVertices[c.face].lon +
                            atan2(sin(Az_earth) * cosLat0SinZ,
                                  cosZ - sinLat0 * sin(lat));

                        revertOrientation({lat, lon}, r);
                    }
                }
            }
            return true;
        }
        r = {inf, inf};
        return false;
    }

  private:
    GeoPoint orientation;
    double cosOrientationLat, sinOrientationLat;

    inline void revertOrientation(const GeoPoint &c, GeoPoint &r) {
        double lon = (c.lat < DEG_TO_RAD * -90 + precisionPerDefinition ||
                      c.lat > DEG_TO_RAD * 90 - precisionPerDefinition)
                         ? 0
                         : c.lon;
        if (orientation.lat != 0.0 || orientation.lon != 0.0) {
            double sinLat = sin(c.lat), cosLat = cos(c.lat);
            double sinLon = sin(lon), cosLon = cos(lon);
            double cosLonCosLat = cosLon * cosLat;
            r = {asin(sinLat * cosOrientationLat -
                      cosLonCosLat * sinOrientationLat),
                 atan2(sinLon * cosLat, cosLonCosLat * cosOrientationLat +
                                            sinLat * sinOrientationLat) -
                     orientation.lon};
        } else
            r = {c.lat, lon};
    }

    static inline double faceOrientation(int face) {
        return (face <= 4 || (10 <= face && face <= 14)) ? 0 : DEG_TO_RAD * 180;
    }
};

// Orientation symmetric to equator (+proj=isea)
/* Sets the orientation of the icosahedron such that the north and the south
 * poles are mapped to the edge midpoints of the icosahedron. The equator is
 * thus mapped symmetrically.
 */
static ISEAPlanarProjection standardISEA(
    /* DEG_TO_RAD * (90 - 58.282525589) = 31.7174744114613 */
    {(E_RAD + F_RAD) / 2, DEG_TO_RAD * -11.25});

// Polar orientation (+proj=isea +orient=pole)
/*
 * One corner of the icosahedron is, by default, facing to the north pole, and
 * one to the south pole. The provided orientation is relative to the default
 * orientation.
 *
 * The orientation shifts every location by the angle orientation.lon in
 * direction of positive longitude, and thereafter by the angle orientation.lat
 * in direction of positive latitude.
 */
static ISEAPlanarProjection polarISEA({0, 0});

void pj_isea_data::initialize(const PJ *P) {
    struct pj_isea_data *Q = static_cast<struct pj_isea_data *>(P->opaque);
    // Only supporting default planar options for now
    if (Q->output == ISEA_PLANE && Q->o_az == 0.0 && Q->aperture == 3.0 &&
        Q->resolution == 4.) {
        // Only supporting +orient=isea and +orient=pole for now
        if (Q->o_lat == ISEA_STD_LAT && Q->o_lon == ISEA_STD_LONG)
            p = &standardISEA;
        else if (Q->o_lat == M_PI / 2.0 && Q->o_lon == 0)
            p = &polarISEA;
        else
            p = nullptr;
    }

    if (p != nullptr) {
        if (P->e > 0) {
            double a2 = P->a * P->a, c2 = P->b * P->b;
            double log1pe_1me = log((1 + P->e) / (1 - P->e));
            double S = M_PI * (2 * a2 + c2 / P->e * log1pe_1me);
            R2 = S / (4 * M_PI);             // [WGS84] R = 6371007.1809184747 m
            Rprime = RprimeOverR * sqrt(R2); // R'
        } else {
            R2 = P->a * P->a;            // R^2
            Rprime = RprimeOverR * P->a; // R'
        }

        Rprime2X = 2 * Rprime;
        RprimeTang = Rprime * tang; // twice the center-to-base distance
        centerToBase = RprimeTang / 2;
        triWidth = RprimeTang * SQRT3;
        Rprime2Tan2g = RprimeTang * RprimeTang;

        yOffsets[0] = -2 * centerToBase;
        yOffsets[1] = -4 * centerToBase;
        yOffsets[2] = -5 * centerToBase;
        yOffsets[3] = -7 * centerToBase;

        xo = 2.5 * triWidth;
        yo = -1.5 * centerToBase;
        sx = 1.0 / triWidth;
        sy = 1.0 / (3 * centerToBase);
    }
}

} // anonymous namespace

static PJ_LP isea_s_inverse(PJ_XY xy, PJ *P) {
    const struct pj_isea_data *Q =
        static_cast<struct pj_isea_data *>(P->opaque);
    ISEAPlanarProjection *p = Q->p;

    if (p) {
        // Default origin of +proj=isea is different (OGC:1534 is
        // +x_0=19186144.870934911 +y_0=-3323137.7717836285)
        PJ_XY input{xy.x * P->a + Q->xo, xy.y * P->a + Q->yo};
        GeoPoint result;

        if (p->cartesianToGeo(input, Q, result))
            //  TODO: Convert authalic latitude to geodetic latitude if not
            //        spherical as in eqearth, healpix, laea, etc.
            return {result.lon, result.lat};
        else
            return {inf, inf};
    } else
        return {inf, inf};
}

#undef ISEA_STD_LAT
#undef ISEA_STD_LONG

#undef numIcosahedronFaces
#undef precision
#undef precisionPerDefinition

#undef AzMax
#undef sdc2vos
#undef tang
#undef cotTheta
#undef cosG
#undef sinGcosSDC2VoS
#undef westVertexLon

#undef RprimeOverR

#undef Min
#undef Max

#undef inf

#undef E_RAD
#undef F_RAD

#undef DEG36
#undef DEG72
#undef DEG90
#undef DEG108
#undef DEG120
#undef DEG144
#undef DEG180
#undef ISEA_SCALE
#undef V_LAT
#undef TABLE_G
#undef TABLE_H
