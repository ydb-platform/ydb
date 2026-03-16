/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_space_view.h"

eccodes::geo_iterator::SpaceView _grib_iterator_space_view{};
eccodes::geo_iterator::Iterator* grib_iterator_space_view = &_grib_iterator_space_view;

namespace eccodes::geo_iterator {

#define ITER "Space view Geoiterator"

int SpaceView::next(double* lat, double* lon, double* val) const
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

// static void adjustBadlyEncodedEcmwfGribs(grib_handle* h,
//                                     long* nx, long* ny, double* dx, double* dy, double* xp, double* yp)
// {
//     /* Correct the information provided in the headers of certain satellite imagery that
//      * we have available. This is specific to ECMWF.
//      * Obtained through trial-and-error to get the best match with the coastlines.
//      *
//      * Copied from Magics GribSatelliteInterpretor::AdjustBadlyEncodedGribs()
//      */
//     long centre = 0;
//     int err     = grib_get_long(h, "centre", &centre);
//     if (!err && centre == 98) {
//         int err1 = 0, err2 = 0, err3 = 0;
//         long satelliteIdentifier, channelNumber, functionCode;
//         /* These keys are defined in the ECMWF local definition 24 - Satellite image simulation */
//         err1 = grib_get_long(h, "satelliteIdentifier", &satelliteIdentifier);
//         err2 = grib_get_long(h, "channelNumber", &channelNumber);
//         err3 = grib_get_long(h, "functionCode", &functionCode);
//         if (!err1 && !err2 && !err3) {
//             if (satelliteIdentifier == 54 && channelNumber == 2 && *dx == 1179) { /* Meteosat 7, channel 2 */
//                 *nx = *ny = 900;
//                 *dx = *dy = 853;
//                 *xp = *yp = 450;
//             }
//             else if (satelliteIdentifier == 54 && channelNumber == 3 && *dx == 1179) { /* Meteosat 7, channel 3 */
//                 *dx = *dy = 1184;
//                 *xp = *yp = 635;
//             }
//             else if (satelliteIdentifier == 259 && channelNumber == 4 && *dx == 1185) { /* GOES-15 (West) channel 4 */
//                 *dx = *dy = 880;
//                 *xp = *yp = 450;
//             }
//             else if (satelliteIdentifier == 57 && *dx == 1732) { /* MSG (Meteosat second generation), non-HRV channels */
//                 *dx = *dy = 1811;
//                 *xp = *yp = 928;
//             }
//         }
//     }
// }

#define RAD2DEG 57.29577951308232087684 /* 180 over pi */
#define DEG2RAD 0.01745329251994329576  /* pi over 180 */

int SpaceView::init(grib_handle* h, grib_arguments* args)
{
    int ret = GRIB_SUCCESS;
    if ((ret = Gen::init(h, args)) != GRIB_SUCCESS)
        return ret;

    /* REFERENCE:
     *  LRIT/HRIT Global Specification (CGMS 03, Issue 2.6, 12.08.1999)
     */
    double *lats, *lons; /* arrays of latitudes and longitudes */
    double latOfSubSatellitePointInDegrees, lonOfSubSatellitePointInDegrees;
    double orientationInDegrees, nrInRadiusOfEarth;
    double radius = 0, xpInGridLengths = 0, ypInGridLengths = 0;
    long nx, ny, earthIsOblate                              = 0;
    long alternativeRowScanning, iScansNegatively;
    long Xo, Yo, jScansPositively, jPointsAreConsecutive, i;

    double major = 0, minor = 0, r_eq, r_pol, height;
    double lap, lop, angular_size;
    double xp, yp, dx, dy, rx, ry, x, y;
    double cos_x, cos_y, sin_x, sin_y;
    double factor_1, factor_2, tmp1, Sd, Sn, Sxy, S1, S2, S3;
    int x0, y0, ix, iy;
    double *s_x, *c_x; /* arrays storing sin and cos values */
    size_t array_size = (nv_ * sizeof(double));

    const char* sradius                          = args->get_name(h, carg_++);
    const char* sEarthIsOblate                   = args->get_name(h, carg_++);
    const char* sMajorAxisInMetres               = args->get_name(h, carg_++);
    const char* sMinorAxisInMetres               = args->get_name(h, carg_++);
    const char* snx                              = args->get_name(h, carg_++);
    const char* sny                              = args->get_name(h, carg_++);
    const char* sLatOfSubSatellitePointInDegrees = args->get_name(h, carg_++);
    const char* sLonOfSubSatellitePointInDegrees = args->get_name(h, carg_++);
    const char* sDx                              = args->get_name(h, carg_++);
    const char* sDy                              = args->get_name(h, carg_++);
    const char* sXpInGridLengths                 = args->get_name(h, carg_++);
    const char* sYpInGridLengths                 = args->get_name(h, carg_++);
    const char* sOrientationInDegrees            = args->get_name(h, carg_++);
    const char* sNrInRadiusOfEarthScaled         = args->get_name(h, carg_++);
    const char* sXo                              = args->get_name(h, carg_++);
    const char* sYo                              = args->get_name(h, carg_++);

    const char* siScansNegatively       = args->get_name(h, carg_++);
    const char* sjScansPositively       = args->get_name(h, carg_++);
    const char* sjPointsAreConsecutive  = args->get_name(h, carg_++);
    const char* sAlternativeRowScanning = args->get_name(h, carg_++);

    if ((ret = grib_get_long_internal(h, snx, &nx)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, sny, &ny)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, sEarthIsOblate, &earthIsOblate)) != GRIB_SUCCESS)
        return ret;

    if (earthIsOblate) {
        if ((ret = grib_get_double_internal(h, sMajorAxisInMetres, &major)) != GRIB_SUCCESS)
            return ret;
        if ((ret = grib_get_double_internal(h, sMinorAxisInMetres, &minor)) != GRIB_SUCCESS)
            return ret;
    }
    else {
        if ((ret = grib_get_double_internal(h, sradius, &radius)) != GRIB_SUCCESS)
            return ret;
    }

    if (nv_ != nx * ny) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Wrong number of points (%zu!=%ldx%ld)", ITER, nv_, nx, ny);
        return GRIB_WRONG_GRID;
    }
    if ((ret = grib_get_double_internal(h, sLatOfSubSatellitePointInDegrees, &latOfSubSatellitePointInDegrees)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, sLonOfSubSatellitePointInDegrees, &lonOfSubSatellitePointInDegrees)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, sDx, &dx)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, sDy, &dy)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, sXpInGridLengths, &xpInGridLengths)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, sYpInGridLengths, &ypInGridLengths)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, sOrientationInDegrees, &orientationInDegrees)) != GRIB_SUCCESS)
        return ret;

    /* Orthographic not supported. This happens when Nr (camera altitude) is missing */
    if (grib_is_missing(h, "Nr", &ret)) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Orthographic view (Nr missing) not supported", ITER);
        return GRIB_GEOCALCULUS_PROBLEM;
    }
    if ((ret = grib_get_double_internal(h, sNrInRadiusOfEarthScaled, &nrInRadiusOfEarth)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(h, sXo, &Xo)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, sYo, &Yo)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, sjPointsAreConsecutive, &jPointsAreConsecutive)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, sjScansPositively, &jScansPositively)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, siScansNegatively, &iScansNegatively)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, sAlternativeRowScanning, &alternativeRowScanning)) != GRIB_SUCCESS)
        return ret;

    if (earthIsOblate) {
        r_eq  = major; /* In km */
        r_pol = minor;
    }
    else {
        r_eq = r_pol = radius * 0.001; /*conv to km*/
    }

    if (nrInRadiusOfEarth == 0) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Key %s must be greater than zero", ITER, sNrInRadiusOfEarthScaled);
        return GRIB_GEOCALCULUS_PROBLEM;
    }

    angular_size = 2.0 * asin(1.0 / nrInRadiusOfEarth);
    height       = nrInRadiusOfEarth * r_eq;

    lap = latOfSubSatellitePointInDegrees;
    lop = lonOfSubSatellitePointInDegrees;
    if (lap != 0.0) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "%s: Key %s must be 0 (satellite must be located in the equator plane)",
                         ITER, sLatOfSubSatellitePointInDegrees);
        return GRIB_GEOCALCULUS_PROBLEM;
    }

    /*orient_angle = orientationInDegrees;*/
    /* if (orient_angle != 0.0) return GRIB_NOT_IMPLEMENTED; */

    xp = xpInGridLengths;
    yp = ypInGridLengths;
    x0 = Xo;
    y0 = Yo;

    /* adjustBadlyEncodedEcmwfGribs(h, &nx, &ny, &dx, &dy, &xp, &yp); */
    if (dx == 0 || dy == 0) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Keys %s and %s must be greater than zero", ITER, sDx, sDy);
        return GRIB_GEOCALCULUS_PROBLEM;
    }
    rx = angular_size / dx;
    ry = (r_pol / r_eq) * angular_size / dy;

    lats_ = (double*)grib_context_malloc(h->context, array_size);
    if (!lats_) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, array_size);
        return GRIB_OUT_OF_MEMORY;
    }
    lons_ = (double*)grib_context_malloc(h->context, array_size);
    if (!lons_) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, array_size);
        return GRIB_OUT_OF_MEMORY;
    }
    lats = lats_;
    lons = lons_;

    if (!iScansNegatively) {
        xp = xp - x0;
    }
    else {
        xp = (nx - 1) - (xp - x0);
    }
    if (jScansPositively) {
        yp = yp - y0;
    }
    else {
        yp = (ny - 1) - (yp - y0);
    }
    i        = 0;
    factor_2 = (r_eq / r_pol) * (r_eq / r_pol);
    factor_1 = height * height - r_eq * r_eq;

    /* Store array of sin and cosine values to avoid recalculation */
    s_x = (double*)grib_context_malloc(h->context, nx * sizeof(double));
    if (!s_x) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, nx * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }
    c_x = (double*)grib_context_malloc(h->context, nx * sizeof(double));
    if (!c_x) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, nx * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }

    for (ix = 0; ix < nx; ix++) {
        x       = (ix - xp) * rx;
        s_x[ix] = sin(x);
        c_x[ix] = sqrt(1.0 - s_x[ix] * s_x[ix]);
    }

    /*for (iy = 0; iy < ny; iy++) {*/
    for (iy = ny - 1; iy >= 0; --iy) {
        y     = (iy - yp) * ry;
        sin_y = sin(y);
        cos_y = sqrt(1.0 - sin_y * sin_y);

        tmp1 = (1 + (factor_2 - 1.0) * sin_y * sin_y);

        for (ix = 0; ix < nx; ix++, i++) {
            /*x = (ix - xp) * rx;*/
            /* Use sin/cos previously computed */
            sin_x = s_x[ix];
            cos_x = c_x[ix];

            Sd = height * cos_x * cos_y;
            Sd = Sd * Sd - tmp1 * factor_1;
            if (Sd <= 0.0) {           /* outside of view */
                lats[i] = lons[i] = 0; /* TODO: error? */
            }
            else {
                Sd      = sqrt(Sd);
                Sn      = (height * cos_x * cos_y - Sd) / tmp1;
                S1      = height - Sn * cos_x * cos_y;
                S2      = Sn * sin_x * cos_y;
                S3      = Sn * sin_y;
                Sxy     = sqrt(S1 * S1 + S2 * S2);
                lons[i] = atan(S2 / S1) * (RAD2DEG) + lop;
                lats[i] = atan(factor_2 * S3 / Sxy) * (RAD2DEG);
                /*fprintf(stderr, "lat=%g   lon=%g\n", lats[i], lons[i]);*/
            }
            while (lons[i] < 0)
                lons[i] += 360;
            while (lons[i] > 360)
                lons[i] -= 360;
        }
    }
    grib_context_free(h->context, s_x);
    grib_context_free(h->context, c_x);
    e_ = -1;

    return ret;
}

int SpaceView::destroy()
{
    DEBUG_ASSERT(h_);
    const grib_context* c = h_->context;
    grib_context_free(c, lats_);
    grib_context_free(c, lons_);

    return Gen::destroy();
}

}  // namespace eccodes::geo_iterator
