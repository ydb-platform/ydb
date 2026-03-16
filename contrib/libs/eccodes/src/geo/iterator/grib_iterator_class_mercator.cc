/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_mercator.h"

eccodes::geo_iterator::Mercator _grib_iterator_mercator{};
eccodes::geo_iterator::Iterator* grib_iterator_mercator = &_grib_iterator_mercator;

namespace eccodes::geo_iterator {

#define ITER    "Mercator Geoiterator"
#define EPSILON 1.0e-10

#ifndef M_PI
    #define M_PI 3.14159265358979323846 /* Whole pie */
#endif

#ifndef M_PI_2
    #define M_PI_2 1.57079632679489661923 /* Half a pie */
#endif

#ifndef M_PI_4
    #define M_PI_4 0.78539816339744830962 /* Quarter of a pie */
#endif

#define RAD2DEG 57.29577951308232087684 /* 180 over pi */
#define DEG2RAD 0.01745329251994329576  /* pi over 180 */

/* Adjust longitude (in radians) to range -180 to 180 */
static double adjust_lon_radians(double lon)
{
    if (lon > M_PI) lon -= 2 * M_PI;
    if (lon < -M_PI) lon += 2 * M_PI;
    return lon;
}

/* Function to compute the latitude angle, phi2, for the inverse
 * From the book "Map Projections-A Working Manual-John P. Snyder (1987)"
 * Equation (7-9) involves rapidly converging iteration: Calculate t from (15-11)
 * Then, assuming an initial trial phi equal to (pi/2 - 2*arctan t) in the right side of equation (7-9),
 * calculate phi on the left side. Substitute the calculated phi into the right side,
 * calculate a new phi, etc., until phi does not change significantly from the preceding trial value of phi
 */
static double compute_phi(
    double eccent, /* Spheroid eccentricity */
    double ts,     /* Constant value t */
    int* error)
{
    double eccnth, phi, con, dphi, sinpi;
    int i, MAX_ITER = 15;

    eccnth = 0.5 * eccent;
    phi    = M_PI_2 - 2 * atan(ts);
    for (i = 0; i <= MAX_ITER; i++) {
        sinpi = sin(phi);
        con   = eccent * sinpi;
        dphi  = M_PI_2 - 2 * atan(ts * (pow(((1.0 - con) / (1.0 + con)), eccnth))) - phi;
        phi += dphi;
        if (fabs(dphi) <= EPSILON)
            return phi;
    }
    *error = GRIB_INTERNAL_ERROR;
    return 0;
}

/* Compute the constant small t for use in the forward computations */
static double compute_t(
    double eccent, /* Eccentricity of the spheroid */
    double phi,    /* Latitude phi */
    double sinphi) /* Sine of the latitude */
{
    double con = eccent * sinphi;
    double com = 0.5 * eccent;
    con        = pow(((1.0 - con) / (1.0 + con)), com);
    return (tan(0.5 * (M_PI_2 - phi)) / con);
}

int Mercator::init_mercator(grib_handle* h,
                            size_t nv, long nx, long ny,
                            double DiInMetres, double DjInMetres,
                            double earthMinorAxisInMetres, double earthMajorAxisInMetres,
                            double latFirstInRadians, double lonFirstInRadians,
                            double latLastInRadians, double lonLastInRadians,
                            double LaDInRadians, double orientationInRadians)
{
    int i, j, err = 0;
    double x0, y0, x, y, latRad, lonRad, latDeg, lonDeg, sinphi, ts;
    double false_easting;  /* x offset in meters */
    double false_northing; /* y offset in meters */
    double m1;             /* small value m */
    double temp, e, es;

    temp = earthMinorAxisInMetres / earthMajorAxisInMetres;
    es   = 1.0 - (temp * temp);
    e    = sqrt(es);
    m1   = cos(LaDInRadians) / (sqrt(1.0 - es * sin(LaDInRadians) * sin(LaDInRadians)));

    /* Forward projection: convert lat,lon to x,y */
    if (fabs(fabs(latFirstInRadians) - M_PI_2) <= EPSILON) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Transformation cannot be computed at the poles", ITER);
        return GRIB_GEOCALCULUS_PROBLEM;
    }
    else {
        sinphi = sin(latFirstInRadians);
        ts     = compute_t(e, latFirstInRadians, sinphi);
        x0     = earthMajorAxisInMetres * m1 * adjust_lon_radians(lonFirstInRadians - orientationInRadians);
        y0     = 0 - earthMajorAxisInMetres * m1 * log(ts);
    }
    x0 = -x0;
    y0 = -y0;

    /* Allocate latitude and longitude arrays */
    lats_ = (double*)grib_context_malloc(h->context, nv * sizeof(double));
    if (!lats_) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, nv * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }
    lons_ = (double*)grib_context_malloc(h->context, nv * sizeof(double));
    if (!lons_) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, nv * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }

    /* Populate our arrays */
    false_easting  = x0;
    false_northing = y0;
    for (j = 0; j < ny; j++) {
        y = j * DjInMetres;
        for (i = 0; i < nx; i++) {
            const int index = i + j * nx;
            double _x, _y;
            x = i * DiInMetres;
            /* Inverse projection to convert from x,y to lat,lon */
            _x     = x - false_easting;
            _y     = y - false_northing;
            ts     = exp(-_y / (earthMajorAxisInMetres * m1));
            latRad = compute_phi(e, ts, &err);
            if (err) {
                grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Failed to compute the latitude angle, phi2, for the inverse", ITER);
                grib_context_free(h->context, lats_);
                grib_context_free(h->context, lons_);
                return err;
            }
            lonRad = adjust_lon_radians(orientationInRadians + _x / (earthMajorAxisInMetres * m1));
            if (i == 0 && j == 0) {
                DEBUG_ASSERT(fabs(latFirstInRadians - latRad) <= EPSILON);
            }
            latDeg       = latRad * RAD2DEG; /* Convert to degrees */
            lonDeg       = normalise_longitude_in_degrees(lonRad * RAD2DEG);
            lons_[index] = lonDeg;
            lats_[index] = latDeg;
        }
    }
    return GRIB_SUCCESS;
}

int Mercator::init(grib_handle* h, grib_arguments* args)
{
    int err = GRIB_SUCCESS;
    if ((err = Gen::init(h, args)) != GRIB_SUCCESS)
        return err;

    long ni, nj, iScansNegatively, jScansPositively, jPointsAreConsecutive, alternativeRowScanning;
    double latFirstInDegrees, lonFirstInDegrees, LaDInDegrees;
    double latLastInDegrees, lonLastInDegrees, orientationInDegrees, DiInMetres, DjInMetres, radius = 0;
    double latFirstInRadians, lonFirstInRadians, latLastInRadians, lonLastInRadians,
        LaDInRadians, orientationInRadians;
    double earthMajorAxisInMetres = 0, earthMinorAxisInMetres = 0;

    const char* sRadius               = args->get_name(h, carg_++);
    const char* sNi                   = args->get_name(h, carg_++);
    const char* sNj                   = args->get_name(h, carg_++);
    const char* sLatFirstInDegrees    = args->get_name(h, carg_++);
    const char* sLonFirstInDegrees    = args->get_name(h, carg_++);
    const char* sLaDInDegrees         = args->get_name(h, carg_++);
    const char* sLatLastInDegrees     = args->get_name(h, carg_++);
    const char* sLonLastInDegrees     = args->get_name(h, carg_++);
    const char* sOrientationInDegrees = args->get_name(h, carg_++);
    /* Dx and Dy are in Metres */
    const char* sDi                     = args->get_name(h, carg_++);
    const char* sDj                     = args->get_name(h, carg_++);
    const char* siScansNegatively       = args->get_name(h, carg_++);
    const char* sjScansPositively       = args->get_name(h, carg_++);
    const char* sjPointsAreConsecutive  = args->get_name(h, carg_++);
    const char* sAlternativeRowScanning = args->get_name(h, carg_++);

    if ((err = grib_get_long_internal(h, sNi, &ni)) != GRIB_SUCCESS) return err;
    if ((err = grib_get_long_internal(h, sNj, &nj)) != GRIB_SUCCESS) return err;

    if (grib_is_earth_oblate(h)) {
        if ((err = grib_get_double_internal(h, "earthMinorAxisInMetres", &earthMinorAxisInMetres)) != GRIB_SUCCESS) return err;
        if ((err = grib_get_double_internal(h, "earthMajorAxisInMetres", &earthMajorAxisInMetres)) != GRIB_SUCCESS) return err;
    }
    else {
        if ((err = grib_get_double_internal(h, sRadius, &radius)) != GRIB_SUCCESS) return err;
        earthMinorAxisInMetres = earthMajorAxisInMetres = radius;
    }

    if (nv_ != ni * nj) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Wrong number of points (%zu!=%ldx%ld)", ITER, nv_, ni, nj);
        return GRIB_WRONG_GRID;
    }

    if ((err = grib_get_double_internal(h, sLaDInDegrees, &LaDInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sLatFirstInDegrees, &latFirstInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sLonFirstInDegrees, &lonFirstInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sLatLastInDegrees, &latLastInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sLonLastInDegrees, &lonLastInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sOrientationInDegrees, &orientationInDegrees)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_double_internal(h, sDi, &DiInMetres)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sDj, &DjInMetres)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, sjPointsAreConsecutive, &jPointsAreConsecutive)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, sjScansPositively, &jScansPositively)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, siScansNegatively, &iScansNegatively)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, sAlternativeRowScanning, &alternativeRowScanning)) != GRIB_SUCCESS)
        return err;

    latFirstInRadians    = latFirstInDegrees * DEG2RAD;
    lonFirstInRadians    = lonFirstInDegrees * DEG2RAD;
    latLastInRadians     = latLastInDegrees * DEG2RAD;
    lonLastInRadians     = lonLastInDegrees * DEG2RAD;
    LaDInRadians         = LaDInDegrees * DEG2RAD;
    orientationInRadians = orientationInDegrees * DEG2RAD;

    err = init_mercator(h, nv_, ni, nj,
                        DiInMetres, DjInMetres, earthMinorAxisInMetres, earthMajorAxisInMetres,
                        latFirstInRadians, lonFirstInRadians,
                        latLastInRadians, lonLastInRadians,
                        LaDInRadians, orientationInRadians);
    if (err) return err;

    e_ = -1;

    /* Apply the scanning mode flags which may require data array to be transformed */
    err = transform_iterator_data(h->context, data_,
                                  iScansNegatively, jScansPositively, jPointsAreConsecutive, alternativeRowScanning,
                                  nv_, ni, nj);
    return err;
}

int Mercator::next(double* lat, double* lon, double* val) const
{
    if ((long)e_ >= (long)(nv_ - 1))
        return 0;
    e_++;

    *lat = lats_[e_];
    *lon = lons_[e_];
    if (val && data_) {
        *val = data_[e_];
    }
    return 1;
}

int Mercator::destroy()
{
    DEBUG_ASSERT(h_);
    const grib_context* c = h_->context;
    grib_context_free(c, lats_);
    grib_context_free(c, lons_);

    return Gen::destroy();
}

}  // namespace eccodes::geo_iterator
