/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/***************************************************************************
 *   Jean Baptiste Filippi - 01.11.2005                                    *
 ***************************************************************************/
#include "grib_api_internal.h"

#include <cmath>
#include <algorithm>

#define MAXITER 10

#define RAD2DEG 57.29577951308232087684 /* 180 over pi */
#define DEG2RAD 0.01745329251994329576  /* pi over 180 */

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

static void gauss_first_guess(long trunc, double* vals)
{
    long i = 0, numVals;
    static double gvals[] = {
        2.4048255577E0,
        5.5200781103E0,
        8.6537279129E0,
        11.7915344391E0,
        14.9309177086E0,
        18.0710639679E0,
        21.2116366299E0,
        24.3524715308E0,
        27.4934791320E0,
        30.6346064684E0,
        33.7758202136E0,
        36.9170983537E0,
        40.0584257646E0,
        43.1997917132E0,
        46.3411883717E0,
        49.4826098974E0,
        52.6240518411E0,
        55.7655107550E0,
        58.9069839261E0,
        62.0484691902E0,
        65.1899648002E0,
        68.3314693299E0,
        71.4729816036E0,
        74.6145006437E0,
        77.7560256304E0,
        80.8975558711E0,
        84.0390907769E0,
        87.1806298436E0,
        90.3221726372E0,
        93.4637187819E0,
        96.6052679510E0,
        99.7468198587E0,
        102.8883742542E0,
        106.0299309165E0,
        109.1714896498E0,
        112.3130502805E0,
        115.4546126537E0,
        118.5961766309E0,
        121.7377420880E0,
        124.8793089132E0,
        128.0208770059E0,
        131.1624462752E0,
        134.3040166383E0,
        137.4455880203E0,
        140.5871603528E0,
        143.7287335737E0,
        146.8703076258E0,
        150.0118824570E0,
        153.1534580192E0,
        156.2950342685E0,
    };

    numVals = sizeof(gvals) / sizeof(gvals[0]);
    for (i = 0; i < trunc; i++) {
        if (i < numVals)
            vals[i] = gvals[i];
        else
            vals[i] = vals[i - 1] + M_PI;
    }
}

/* 'trunc' is the Gaussian number (or order) */
/* i.e. Number of parallels between a pole and the equator. */
/* The provided 'lats' array should have allocated 2*trunc elements */
static int compute_gaussian_latitudes(long trunc, double* lats)
{
    long jlat, iter, legi;
    double rad2deg, convval, root, legfonc = 0;
    double mem1, mem2, conv;
    double denom     = 0.0;
    double precision = 1.0E-14;
    const long nlat  = trunc * 2;

    rad2deg = 180.0 / M_PI;

    convval = (1.0 - ((2.0 / M_PI) * (2.0 / M_PI)) * 0.25);

    gauss_first_guess(trunc, lats);
    denom = sqrt(((((double)nlat) + 0.5) * (((double)nlat) + 0.5)) + convval);

    for (jlat = 0; jlat < trunc; jlat++) {
        /*   First approximation for root      */
        root = cos(lats[jlat] / denom);

        /*   Perform loop of Newton iterations  */
        iter = 0;
        conv = 1;

        while (fabs(conv) >= precision) {
            mem2 = 1.0;
            mem1 = root;

            /*  Compute Legendre polynomial  */
            for (legi = 0; legi < nlat; legi++) {
                legfonc = ((2.0 * (legi + 1) - 1.0) * root * mem1 - legi * mem2) / ((double)(legi + 1));
                mem2    = mem1;
                mem1    = legfonc;
            }

            /*  Perform Newton iteration  */
            conv = legfonc / ((((double)nlat) * (mem2 - root * legfonc)) / (1.0 - (root * root)));
            root -= conv;

            /*  Routine fails if no convergence after MAXITER iterations  */
            if (iter++ > MAXITER) {
                return GRIB_GEOCALCULUS_PROBLEM;
            }
        }

        /*   Set North and South values using symmetry */
        lats[jlat]            = asin(root) * rad2deg;
        lats[nlat - 1 - jlat] = -lats[jlat];
    }

    //if (nlat != (trunc * 2))
    //    lats[trunc + 1] = 0.0;
    return GRIB_SUCCESS;
}

// Performance: return the precomputed latitudes for N=640
// The provided 'lats' array should have allocated 2*N elements
static int get_precomputed_latitudes_N640(double* lats)
{
    #include "grib_gaussian_N640.h"
    const size_t N = 640;
    for (size_t i = 0; i < N; ++i) {
        lats[i] = lats_N640[i];
    }

    const size_t ilast = N * 2 - 1;
    for (size_t i = ilast; i > ilast / 2; --i) {
        lats[i] = -lats[ilast - i];
    }

    return GRIB_SUCCESS;
}

// Performance: return the precomputed latitudes for N=1280
// The provided 'lats' array should have allocated 2*N elements
static int get_precomputed_latitudes_N1280(double* lats)
{
    #include "grib_gaussian_N1280.h"
    const size_t N = 1280;
    for (size_t i = 0; i < N; ++i) {
        lats[i] = lats_N1280[i];
    }

    const size_t ilast = N * 2 - 1;
    for (size_t i = ilast; i > ilast / 2; --i) {
        lats[i] = -lats[ilast - i];
    }

    return GRIB_SUCCESS;
}

int grib_get_gaussian_latitudes(long trunc, double* lats)
{
    if (trunc <= 0)
        return GRIB_GEOCALCULUS_PROBLEM;

    if (trunc == 640) {
        return get_precomputed_latitudes_N640(lats);
    }
    if (trunc == 1280) {
        return get_precomputed_latitudes_N1280(lats);
    }
    return compute_gaussian_latitudes(trunc, lats);
}

/* Boolean return type: 1 if the reduced gaussian field is global, 0 for sub area */
int is_gaussian_global(
    double lat1, double lat2, double lon1, double lon2, /* bounding box*/
    long num_points_equator,                            /* num points on latitude at equator */
    const double* latitudes,                            /* array of Gaussian latitudes (size 2*N) */
    double angular_precision                            /* tolerance for angle comparison */
)
{
    int global         = 1;
    const double d     = fabs(latitudes[0] - latitudes[1]);
    const double delta = 360.0 / num_points_equator;
    /* Compute the expected last longitude for a global field */
    const double lon2_global = 360.0 - delta;
    /* Compute difference between expected longitude and actual one */
    const double lon2_diff = fabs(lon2 - lon2_global) - delta;

    /*
    {
        grib_context* c=grib_context_get_default();
        if (c->debug) {
            fprintf(stderr,"ECCODES DEBUG is_gaussian_global: lat1=%f, lat2=%f, glat0=%f, d=%f\n", lat1, lat2, latitudes[0], d);
            fprintf(stderr,"ECCODES DEBUG is_gaussian_global: lon1=%f, lon2=%f, glon2=%f, delta=%f\n", lon1, lon2, lon2_global, delta);
        }
    }
    */

    /* Note: final gaussian latitude = -first latitude */
    if ((fabs(lat1 - latitudes[0]) >= d) ||
        (fabs(lat2 + latitudes[0]) >= d) ||
        lon1 != 0 ||
        lon2_diff > angular_precision) {
        global = 0; /* sub area */
    }
    return global;
}

/* From Magics GribRotatedInterpretor::rotate */
void rotate(const double inlat, const double inlon,
            const double angleOfRot, const double southPoleLat, const double southPoleLon,
            double* outlat, double* outlon)
{
    double PYROT, PXROT, ZCYROT, ZCXROT, ZSXROT;
    const double ZSYCEN = sin(DEG2RAD * (southPoleLat + 90.));
    const double ZCYCEN = cos(DEG2RAD * (southPoleLat + 90.));
    const double ZXMXC  = DEG2RAD * (inlon - southPoleLon);
    const double ZSXMXC = sin(ZXMXC);
    const double ZCXMXC = cos(ZXMXC);
    const double ZSYREG = sin(DEG2RAD * inlat);
    const double ZCYREG = cos(DEG2RAD * inlat);
    double ZSYROT = ZCYCEN * ZSYREG - ZSYCEN * ZCYREG * ZCXMXC;

    ZSYROT = std::max(std::min(ZSYROT, +1.0), -1.0);

    PYROT = asin(ZSYROT) * RAD2DEG;

    ZCYROT = cos(PYROT * DEG2RAD);
    ZCXROT = (ZCYCEN * ZCYREG * ZCXMXC + ZSYCEN * ZSYREG) / ZCYROT;
    ZCXROT = std::max(std::min(ZCXROT, +1.0), -1.0);
    ZSXROT = ZCYREG * ZSXMXC / ZCYROT;

    PXROT = acos(ZCXROT) * RAD2DEG;

    if (ZSXROT < 0.0)
        PXROT = -PXROT;

    *outlat = PYROT;
    *outlon = PXROT;
}

/* From old ecKit RotateGrid::unrotate (Tag 2015.11.0) */
void unrotate(const double inlat, const double inlon,
              const double angleOfRot, const double southPoleLat, const double southPoleLon,
              double* outlat, double* outlon)
{
    const double lon_x = inlon;
    const double lat_y = inlat;
    /* First convert the data point from spherical lat lon to (x',y',z') */
    double latr = lat_y * DEG2RAD;
    double lonr = lon_x * DEG2RAD;
    double xd   = cos(lonr) * cos(latr);
    double yd   = sin(lonr) * cos(latr);
    double zd   = sin(latr);

    double t = -(90.0 + southPoleLat);
    double o = -southPoleLon;

    double sin_t = sin(DEG2RAD * t);
    double cos_t = cos(DEG2RAD * t);
    double sin_o = sin(DEG2RAD * o);
    double cos_o = cos(DEG2RAD * o);

    double x = cos_t * cos_o * xd + sin_o * yd + sin_t * cos_o * zd;
    double y = -cos_t * sin_o * xd + cos_o * yd - sin_t * sin_o * zd;
    double z = -sin_t * xd + cos_t * zd;

    double ret_lat = 0, ret_lon = 0;

    /* Then convert back to 'normal' (lat,lon)
     * Uses arcsin, to convert back to degrees, put in range -1 to 1 in case of slight rounding error
     * avoid error on calculating e.g. asin(1.00000001) */
    if (z > 1.0)
        z = 1.0;
    if (z < -1.0)
        z = -1.0;

    ret_lat = asin(z) * RAD2DEG;
    ret_lon = atan2(y, x) * RAD2DEG;

    /* Still get a very small rounding error, round to 6 decimal places */
    ret_lat = roundf(ret_lat * 1000000.0) / 1000000.0;
    ret_lon = roundf(ret_lon * 1000000.0) / 1000000.0;

    ret_lon -= angleOfRot;

    /* Make sure ret_lon is in range*/
    /*
    while (ret_lon < lonmin_) ret_lon += 360.0;
    while (ret_lon >= lonmax_) ret_lon -= 360.0;
     */
    *outlat = ret_lat;
    *outlon = ret_lon;
}

#define RADIAN(x) ((x)*acos(0.0) / 90.0)

/* radius is in km, angles in degrees */
/* Spherical Law of Cosines */
double geographic_distance_spherical(double radius, double lon1, double lat1, double lon2, double lat2)
{
    double rlat1 = RADIAN(lat1);
    double rlat2 = RADIAN(lat2);
    double rlon1 = lon1;
    double rlon2 = lon2;
    double a;

    if (lat1 == lat2 && lon1 == lon2) {
        return 0.0; /* the two points are identical */
    }
    if (rlon1 >= 360) rlon1 -= 360.0;
    rlon1 = RADIAN(rlon1);
    if (rlon2 >= 360) rlon2 -= 360.0;
    rlon2 = RADIAN(rlon2);

    a = sin(rlat1) * sin(rlat2) + cos(rlat1) * cos(rlat2) * cos(rlon2 - rlon1);
    /* ECC-1258: sometimes 'a' can be very slightly outside the range [-1,1] */
    if (a > 1.0) a = 1.0;
    if (a < -1.0) a = -1.0;

    return radius * acos(a);
}

// major and minor axes in km, angles in degrees
// double geographic_distance_ellipsoid(double major, double minor, double lon1, double lat1, double lon2, double lat2)
// {
//     /* Lambert's formula */
//     double rlat1 = RADIAN(lat1);
//     double rlat2 = RADIAN(lat2);
//     double rlon1 = RADIAN(lon1);
//     double rlon2 = RADIAN(lon2);
//     double deltaLat = rlat2 - rlat1;
//     double deltaLon = rlon2 - rlon1;
//     double sinDlat = sin(deltaLat/2.0);
//     double sinDlon = sin(deltaLon/2.0);
//     double sin2Dlat = sinDlat*sinDlat;
//     double sin2Dlon = sinDlon*sinDlon;

//     double a = sin2Dlat + cos(rlat1) * cos(rlat2) * sin2Dlon;
//     double c = 2 * atan2(sqrt(a), sqrt(1.0-a));
//     double f = (major - minor)/major; /*flattening*/

//     double latr1 = atan( (1.0-f)*tan(rlat1) ); /*Reduced latitude1*/
//     double latr2 = atan( (1.0-f)*tan(rlat2) ); /*Reduced latitude2*/
//     double P = (latr1+latr2)/2;
//     double Q = (latr2-latr1)/2;
//     double sinP = sin(P);
//     double sin2P = sinP*sinP;
//     double cosQ = cos(Q);
//     double cos2Q = cosQ*cosQ;
//     double cosc2 = cos(c/2);
//     double cos2c2 = cosc2*cosc2;

//     double sinQ = sin(Q);
//     double sin2Q = sinQ*sinQ;
//     double cosP = cos(P);
//     double cos2P = cosP*cosP;
//     double sinc2 = sin(c/2);
//     double sin2c2 = sinc2*sinc2;

//     double X = (c - sin(c))* sin2P * cos2Q / cos2c2;
//     double Y = (c + sin(c))*sin2Q*cos2P/sin2c2;
//     double dist = major * (c - f*(X+Y)/2);
//     return dist;
// }
