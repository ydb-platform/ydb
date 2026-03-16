/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_lambert_conformal.h"
#include <cmath>

eccodes::geo_iterator::LambertConformal _grib_iterator_lambert_conformal{};
eccodes::geo_iterator::Iterator* grib_iterator_lambert_conformal = &_grib_iterator_lambert_conformal;

namespace eccodes::geo_iterator {

#define ITER    "Lambert conformal Geoiterator"
#define EPSILON 1.0e-10

#ifndef M_PI
    #define M_PI 3.14159265358979323846  // Whole pie
#endif

#ifndef M_PI_2
    #define M_PI_2 1.57079632679489661923  // Half a pie
#endif

#ifndef M_PI_4
    #define M_PI_4 0.78539816339744830962  // Quarter of a pie
#endif

#define RAD2DEG 57.29577951308232087684  // 180 over pi
#define DEG2RAD 0.01745329251994329576   // pi over 180

// Adjust longitude (in radians) to range -180 to 180
static double adjust_lon_radians(double lon)
{
    if (lon > M_PI) lon -= 2 * M_PI;
    if (lon < -M_PI) lon += 2 * M_PI;
    return lon;
}

// Function to compute the latitude angle, phi2, for the inverse
// From the book "Map Projections-A Working Manual-John P. Snyder (1987)"
// Equation (7-9) involves rapidly converging iteration: Calculate t from (15-11)
// Then, assuming an initial trial phi equal to (pi/2 - 2*arctan t) in the right side of equation (7-9),
// calculate phi on the left side. Substitute the calculated phi) into the right side,
// calculate a new phi, etc., until phi does not change significantly from the preceding trial value of phi
static double compute_phi(
    double eccent,  // Spheroid eccentricity
    double ts,      // Constant value t
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
        if (fabs(dphi) <= 0.0000000001)
            return (phi);
    }
    *error = GRIB_INTERNAL_ERROR;
    return 0;
}

// Compute the constant small m which is the radius of
// a parallel of latitude, phi, divided by the semimajor axis
static double compute_m(double eccent, double sinphi, double cosphi)
{
    const double con = eccent * sinphi;
    return ((cosphi / (sqrt(1.0 - con * con))));
}

// Compute the constant small t for use in the forward computations
static double compute_t(
    double eccent,  // Eccentricity of the spheroid
    double phi,     // Latitude phi
    double sinphi)  // Sine of the latitude
{
    double con = eccent * sinphi;
    double com = 0.5 * eccent;
    con        = pow(((1.0 - con) / (1.0 + con)), com);
    return (tan(0.5 * (M_PI_2 - phi)) / con);
}

static double calculate_eccentricity(double minor, double major)
{
    const double temp = minor / major;
    return sqrt(1.0 - temp * temp);
}

static void xy2lonlat(double radius, double n, double f, double rho0_bare, double LoVInRadians,
                      double x, double y,
                      double* lonDeg, double* latDeg)
{
    DEBUG_ASSERT(radius > 0);
    DEBUG_ASSERT(n != 0.0);
    x /= radius;
    y /= radius;
    y          = rho0_bare - y;
    double rho = hypot(x, y);
    if (rho != 0.0) {
        if (n < 0.0) {
            rho = -rho;
            x   = -x;
            y   = -y;
        }
        double latRadians = 2. * atan(pow(f / rho, 1.0 / n)) - M_PI_2;
        double lonRadians = atan2(x, y) / n;
        *lonDeg           = (lonRadians + LoVInRadians) * RAD2DEG;
        *latDeg           = latRadians * RAD2DEG;
    }
    else {
        *lonDeg = 0.0;
        *latDeg = (n > 0.0 ? M_PI_2 : -M_PI_2) * RAD2DEG;
    }
}

int LambertConformal::init_sphere(const grib_handle* h,
                                  size_t nv, long nx, long ny,
                                  double LoVInDegrees,
                                  double Dx, double Dy, double radius,
                                  double latFirstInRadians, double lonFirstInRadians,
                                  double LoVInRadians, double Latin1InRadians, double Latin2InRadians,
                                  double LaDInRadians)
{
    double n, x, y;

    if (fabs(Latin1InRadians - Latin2InRadians) < 1E-09) {
        n = sin(Latin1InRadians);
    }
    else {
        n = log(cos(Latin1InRadians) / cos(Latin2InRadians)) /
            log(tan(M_PI_4 + Latin2InRadians / 2.0) / tan(M_PI_4 + Latin1InRadians / 2.0));
    }

    double f         = (cos(Latin1InRadians) * pow(tan(M_PI_4 + Latin1InRadians / 2.0), n)) / n;
    double rho       = radius * f * pow(tan(M_PI_4 + latFirstInRadians / 2.0), -n);
    double rho0_bare = f * pow(tan(M_PI_4 + LaDInRadians / 2.0), -n);
    double rho0      = radius * rho0_bare;  // scaled
    double lonDiff   = lonFirstInRadians - LoVInRadians;

    // Adjust longitude to range -180 to 180
    if (lonDiff > M_PI)
        lonDiff -= 2 * M_PI;
    if (lonDiff < -M_PI)
        lonDiff += 2 * M_PI;
    double angle = n * lonDiff;
    double x0    = rho * sin(angle);
    double y0    = rho0 - rho * cos(angle);
    // Dx = iScansNegatively == 0 ? Dx : -Dx;
    // GRIB-405: Don't change sign of Dy. Latitudes ALWAYS increase from latitudeOfFirstGridPoint
    // Dy = jScansPositively == 1 ? Dy : -Dy;

    // Allocate latitude and longitude arrays
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

    double latDeg = 0, lonDeg = 0;
    for (long j = 0; j < ny; j++) {
        y = y0 + j * Dy;
        for (long i = 0; i < nx; i++) {
            const long index = i + j * nx;
            x                = x0 + i * Dx;
            xy2lonlat(radius, n, f, rho0_bare, LoVInRadians, x, y, &lonDeg, &latDeg);
            lonDeg       = normalise_longitude_in_degrees(lonDeg);
            lons_[index] = lonDeg;
            lats_[index] = latDeg;
        }
    }

#if 0
    for (j = 0; j < ny; j++) {
        y = y0 + j * Dy;
        //if (n < 0) { /* adjustment for southern hemisphere */
        //    y = -y;
        //}
        tmp  = rho0 - y;
        tmp2 = tmp * tmp;
        for (i = 0; i < nx; i++) {
            int index = i + j * nx;
            x = x0 + i * Dx;
            //if (n < 0) { /* adjustment for southern hemisphere */
            //    x = -x;
            //}
            angle = atan2(x, tmp); /* See ECC-524 */
            rho   = sqrt(x * x + tmp2);
            if (n <= 0) rho = -rho;
            lonDeg = LoVInDegrees + (angle / n) * RAD2DEG;
            latDeg = (2.0 * atan(pow(radius * f / rho, 1.0 / n)) - M_PI_2) * RAD2DEG;
            lonDeg = normalise_longitude_in_degrees(lonDeg);
            lons_[index] = lonDeg;
            lats_[index] = latDeg;
        }
    }
#endif
    return GRIB_SUCCESS;
}

// Oblate spheroid
int LambertConformal::init_oblate(const grib_handle* h,
                                  size_t nv, long nx, long ny,
                                  double LoVInDegrees,
                                  double Dx, double Dy,
                                  double earthMinorAxisInMetres, double earthMajorAxisInMetres,
                                  double latFirstInRadians, double lonFirstInRadians,
                                  double LoVInRadians, double Latin1InRadians, double Latin2InRadians,
                                  double LaDInRadians)
{
    int i, j, err = 0;
    double x0, y0, x, y, latRad, lonRad, latDeg, lonDeg, sinphi, ts, rh1, theta;
    double false_easting;   // x offset in meters
    double false_northing;  // y offset in meters

    double ns;      // ratio of angle between meridian
    double F;       // flattening of ellipsoid
    double rh;      // height above ellipsoid
    double sin_po;  // sin value
    double cos_po;  // cos value
    double con;     // temporary variable
    double ms1;     // small m 1
    double ms2;     // small m 2
    double ts0;     // small t 0
    double ts1;     // small t 1
    double ts2;     // small t 2

    double e = calculate_eccentricity(earthMinorAxisInMetres, earthMajorAxisInMetres);

    sin_po = sin(Latin1InRadians);
    cos_po = cos(Latin1InRadians);
    con    = sin_po;
    ms1    = compute_m(e, sin_po, cos_po);
    ts1    = compute_t(e, Latin1InRadians, sin_po);

    sin_po = sin(Latin2InRadians);
    cos_po = cos(Latin2InRadians);
    ms2    = compute_m(e, sin_po, cos_po);
    ts2    = compute_t(e, Latin2InRadians, sin_po);
    sin_po = sin(LaDInRadians);
    ts0    = compute_t(e, LaDInRadians, sin_po);

    if (fabs(Latin1InRadians - Latin2InRadians) > EPSILON) {
        ns = log(ms1 / ms2) / log(ts1 / ts2);
    }
    else {
        ns = con;
    }
    F  = ms1 / (ns * pow(ts1, ns));
    rh = earthMajorAxisInMetres * F * pow(ts0, ns);

    // Forward projection: convert lat,lon to x,y
    con = fabs(fabs(latFirstInRadians) - M_PI_2);
    if (con > EPSILON) {
        sinphi = sin(latFirstInRadians);
        ts     = compute_t(e, latFirstInRadians, sinphi);
        rh1    = earthMajorAxisInMetres * F * pow(ts, ns);
    }
    else {
        con = latFirstInRadians * ns;
        if (con <= 0) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Point cannot be projected: latFirstInRadians=%g", ITER, latFirstInRadians);
            return GRIB_GEOCALCULUS_PROBLEM;
        }
        rh1 = 0;
    }
    theta = ns * adjust_lon_radians(lonFirstInRadians - LoVInRadians);
    x0    = rh1 * sin(theta);
    y0    = rh - rh1 * cos(theta);
    x0    = -x0;
    y0    = -y0;

    // Allocate latitude and longitude arrays
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

    // Populate our arrays
    false_easting  = x0;
    false_northing = y0;
    for (j = 0; j < ny; j++) {
        y = j * Dy;
        for (i = 0; i < nx; i++) {
            const int index = i + j * nx;
            double _x, _y;
            x = i * Dx;
            // Inverse projection to convert from x,y to lat,lon
            _x  = x - false_easting;
            _y  = rh - y + false_northing;
            rh1 = sqrt(_x * _x + _y * _y);
            con = 1.0;
            if (ns <= 0) {
                rh1 = -rh1;
                con = -con;
            }
            theta = 0.0;
            if (rh1 != 0)
                theta = atan2((con * _x), (con * _y));
            if ((rh1 != 0) || (ns > 0.0)) {
                con    = 1.0 / ns;
                ts     = pow((rh1 / (earthMajorAxisInMetres * F)), con);
                latRad = compute_phi(e, ts, &err);
                if (err) {
                    grib_context_log(h->context, GRIB_LOG_ERROR,
                                     "%s: Failed to compute the latitude angle, phi2, for the inverse", ITER);
                    grib_context_free(h->context, lats_);
                    grib_context_free(h->context, lons_);
                    return err;
                }
            }
            else {
                latRad = -M_PI_2;
            }
            lonRad = adjust_lon_radians(theta / ns + LoVInRadians);
            if (i == 0 && j == 0) {
                DEBUG_ASSERT(fabs(latFirstInRadians - latRad) <= EPSILON);
            }
            latDeg       = latRad * RAD2DEG;  // Convert to degrees
            lonDeg       = normalise_longitude_in_degrees(lonRad * RAD2DEG);
            lons_[index] = lonDeg;
            lats_[index] = latDeg;
        }
    }
    return GRIB_SUCCESS;
}

int LambertConformal::init(grib_handle* h, grib_arguments* args)
{
    int err = GRIB_SUCCESS;
    if ((err = Gen::init(h, args)) != GRIB_SUCCESS)
        return err;

    int is_oblate = 0;
    long nx, ny, iScansNegatively, jScansPositively, jPointsAreConsecutive, alternativeRowScanning;
    double LoVInDegrees, LaDInDegrees, Latin1InDegrees, Latin2InDegrees, latFirstInDegrees,
        lonFirstInDegrees, Dx, Dy, radius = 0;
    double latFirstInRadians, lonFirstInRadians, LoVInRadians, Latin1InRadians, Latin2InRadians,
        LaDInRadians;
    double earthMajorAxisInMetres = 0, earthMinorAxisInMetres = 0;

    const char* sradius            = args->get_name(h, carg_++);
    const char* snx                = args->get_name(h, carg_++);
    const char* sny                = args->get_name(h, carg_++);
    const char* sLoVInDegrees      = args->get_name(h, carg_++);
    const char* sLaDInDegrees      = args->get_name(h, carg_++);
    const char* sLatin1InDegrees   = args->get_name(h, carg_++);
    const char* sLatin2InDegrees   = args->get_name(h, carg_++);
    const char* slatFirstInDegrees = args->get_name(h, carg_++);
    const char* slonFirstInDegrees = args->get_name(h, carg_++);
    // Dx and Dy are in Metres
    const char* sDx                     = args->get_name(h, carg_++);
    const char* sDy                     = args->get_name(h, carg_++);
    const char* siScansNegatively       = args->get_name(h, carg_++);
    const char* sjScansPositively       = args->get_name(h, carg_++);
    const char* sjPointsAreConsecutive  = args->get_name(h, carg_++);
    const char* salternativeRowScanning = args->get_name(h, carg_++);

    if ((err = grib_get_long_internal(h, snx, &nx)) != GRIB_SUCCESS) return err;
    if ((err = grib_get_long_internal(h, sny, &ny)) != GRIB_SUCCESS) return err;

    is_oblate = grib_is_earth_oblate(h);

    if (is_oblate) {
        if ((err = grib_get_double_internal(h, "earthMinorAxisInMetres", &earthMinorAxisInMetres)) != GRIB_SUCCESS) return err;
        if ((err = grib_get_double_internal(h, "earthMajorAxisInMetres", &earthMajorAxisInMetres)) != GRIB_SUCCESS) return err;
    }
    else {
        if ((err = grib_get_double_internal(h, sradius, &radius)) != GRIB_SUCCESS) return err;
    }

    if (nv_ != nx * ny) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Wrong number of points (%zu!=%ldx%ld)", ITER, nv_, nx, ny);
        return GRIB_WRONG_GRID;
    }

    if ((err = grib_get_double_internal(h, sLoVInDegrees, &LoVInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sLaDInDegrees, &LaDInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sLatin1InDegrees, &Latin1InDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sLatin2InDegrees, &Latin2InDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, slatFirstInDegrees, &latFirstInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, slonFirstInDegrees, &lonFirstInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sDx, &Dx)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sDy, &Dy)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, sjPointsAreConsecutive, &jPointsAreConsecutive)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, sjScansPositively, &jScansPositively)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, siScansNegatively, &iScansNegatively)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, salternativeRowScanning, &alternativeRowScanning)) != GRIB_SUCCESS)
        return err;

    // Standard Parallels cannot be equal and on opposite sides of the equator
    if (fabs(Latin1InDegrees + Latin2InDegrees) < EPSILON) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "%s: Cannot have equal latitudes for standard parallels on opposite sides of equator", ITER);
        return GRIB_WRONG_GRID;
    }

    //
    // See Wolfram MathWorld: http://mathworld.wolfram.com/LambertConformalConicProjection.html
    //
    latFirstInRadians = latFirstInDegrees * DEG2RAD;
    lonFirstInRadians = lonFirstInDegrees * DEG2RAD;
    Latin1InRadians   = Latin1InDegrees * DEG2RAD;
    Latin2InRadians   = Latin2InDegrees * DEG2RAD;
    LaDInRadians      = LaDInDegrees * DEG2RAD;
    LoVInRadians      = LoVInDegrees * DEG2RAD;

    if (is_oblate) {
        err = init_oblate(h, nv_, nx, ny,
                          LoVInDegrees,
                          Dx, Dy, earthMinorAxisInMetres, earthMajorAxisInMetres,
                          latFirstInRadians, lonFirstInRadians,
                          LoVInRadians, Latin1InRadians, Latin2InRadians,
                          LaDInRadians);
    }
    else {
        err = init_sphere(h, nv_, nx, ny,
                          LoVInDegrees,
                          Dx, Dy, radius,
                          latFirstInRadians, lonFirstInRadians,
                          LoVInRadians, Latin1InRadians, Latin2InRadians, LaDInRadians);
    }
    if (err) return err;

    e_ = -1;

    // Apply the scanning mode flags which may require data array to be transformed
    err = transform_iterator_data(h->context, data_,
                                  iScansNegatively, jScansPositively, jPointsAreConsecutive, alternativeRowScanning,
                                  nv_, nx, ny);
    return err;
}

int LambertConformal::next(double* lat, double* lon, double* val) const
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

int LambertConformal::destroy()
{
    DEBUG_ASSERT(h_);
    const grib_context* c = h_->context;
    grib_context_free(c, lats_);
    grib_context_free(c, lons_);

    return Gen::destroy();
}

}  // namespace eccodes::geo_iterator
