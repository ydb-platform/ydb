/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_polar_stereographic.h"

eccodes::geo_iterator::PolarStereographic _grib_iterator_polar_stereographic{};
eccodes::geo_iterator::Iterator* grib_iterator_polar_stereographic = &_grib_iterator_polar_stereographic;

namespace eccodes::geo_iterator {

#define ITER "Polar stereographic Geoiterator"

int PolarStereographic::next(double* lat, double* lon, double* val) const
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

/* Data struct for Forward and Inverse Projections */
typedef struct proj_data_t
{
    double centre_lon;     /* central longitude */
    double centre_lat;     /* central latitude */
    double sign;           /* sign variable */
    double ind;            /* flag variable */
    double mcs;            /* small m */
    double tcs;            /* small t */
    double false_northing; /* y offset in meters */
    double false_easting;  /* x offset in meters */
} proj_data_t;

#define RAD2DEG   57.29577951308232087684 /* 180 over pi */
#define DEG2RAD   0.01745329251994329576  /* pi over 180 */
#define PI_OVER_2 1.5707963267948966      /* half pi */
#define EPSILON   1.0e-10

int PolarStereographic::init(grib_handle* h, grib_arguments* args)
{
    int ret = GRIB_SUCCESS;
    if ((ret = Gen::init(h, args)) != GRIB_SUCCESS)
        return ret;

    double *lats, *lons; /* arrays for latitudes and longitudes */
    double lonFirstInDegrees, latFirstInDegrees, radius;
    double x, y, Dx, Dy;
    long nx, ny;
    double centralLongitudeInDegrees, centralLatitudeInDegrees;
    long alternativeRowScanning, iScansNegatively, i, j;
    long jScansPositively, jPointsAreConsecutive, southPoleOnPlane;
    double centralLongitude, centralLatitude; /* in radians */
    double con1;                              /* temporary angle */
    double ts;                                /* value of small t */
    double height;                            /* height above ellipsoid */
    double x0, y0, lonFirst, latFirst;
    proj_data_t fwd_proj_data = {
        0,
    };
    proj_data_t inv_proj_data = {
        0,
    };

    const char* s_radius                 = args->get_name(h, carg_++);
    const char* s_nx                     = args->get_name(h, carg_++);
    const char* s_ny                     = args->get_name(h, carg_++);
    const char* s_latFirstInDegrees      = args->get_name(h, carg_++);
    const char* s_lonFirstInDegrees      = args->get_name(h, carg_++);
    const char* s_southPoleOnPlane       = args->get_name(h, carg_++);
    const char* s_centralLongitude       = args->get_name(h, carg_++);
    const char* s_centralLatitude        = args->get_name(h, carg_++);
    const char* s_Dx                     = args->get_name(h, carg_++);
    const char* s_Dy                     = args->get_name(h, carg_++);
    const char* s_iScansNegatively       = args->get_name(h, carg_++);
    const char* s_jScansPositively       = args->get_name(h, carg_++);
    const char* s_jPointsAreConsecutive  = args->get_name(h, carg_++);
    const char* s_alternativeRowScanning = args->get_name(h, carg_++);

    if (grib_is_earth_oblate(h)) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Only supported for spherical earth.", ITER);
        return GRIB_GEOCALCULUS_PROBLEM;
    }

    if ((ret = grib_get_double_internal(h, s_radius, &radius)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, s_nx, &nx)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, s_ny, &ny)) != GRIB_SUCCESS)
        return ret;

    if (nv_ != nx * ny) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Wrong number of points (%zu!=%ldx%ld)", ITER, nv_, nx, ny);
        return GRIB_WRONG_GRID;
    }
    if ((ret = grib_get_double_internal(h, s_latFirstInDegrees, &latFirstInDegrees)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, s_lonFirstInDegrees, &lonFirstInDegrees)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, s_southPoleOnPlane, &southPoleOnPlane)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, s_centralLongitude, &centralLongitudeInDegrees)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, s_centralLatitude, &centralLatitudeInDegrees)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, s_Dx, &Dx)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(h, s_Dy, &Dy)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, s_jPointsAreConsecutive, &jPointsAreConsecutive)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, s_jScansPositively, &jScansPositively)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, s_iScansNegatively, &iScansNegatively)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, s_alternativeRowScanning, &alternativeRowScanning)) != GRIB_SUCCESS)
        return ret;

    centralLongitude = centralLongitudeInDegrees * DEG2RAD;
    centralLatitude  = centralLatitudeInDegrees * DEG2RAD;
    lonFirst         = lonFirstInDegrees * DEG2RAD;
    latFirst         = latFirstInDegrees * DEG2RAD;

    /* Forward projection initialisation */
    fwd_proj_data.false_northing = 0;
    fwd_proj_data.false_easting  = 0;
    fwd_proj_data.centre_lon     = centralLongitude;
    fwd_proj_data.centre_lat     = centralLatitude;
    if (centralLatitude < 0)
        fwd_proj_data.sign = -1.0;
    else
        fwd_proj_data.sign = +1.0;
    fwd_proj_data.ind = 0;
    if (fabs(fabs(centralLatitude) - PI_OVER_2) > EPSILON) {
        /* central latitude different from 90 i.e. not north/south polar */
        fwd_proj_data.ind = 1;
        con1              = fwd_proj_data.sign * centralLatitude;
        fwd_proj_data.mcs = cos(con1);
        fwd_proj_data.tcs = tan(0.5 * (PI_OVER_2 - con1));
    }

    /* Forward projection from initial lat,lon to initial x,y */
    con1 = fwd_proj_data.sign * (lonFirst - fwd_proj_data.centre_lon);
    ts   = tan(0.5 * (PI_OVER_2 - fwd_proj_data.sign * latFirst));
    if (fwd_proj_data.ind)
        height = radius * fwd_proj_data.mcs * ts / fwd_proj_data.tcs;
    else
        height = 2.0 * radius * ts;
    x0 = fwd_proj_data.sign * height * sin(con1) + fwd_proj_data.false_easting;
    y0 = -fwd_proj_data.sign * height * cos(con1) + fwd_proj_data.false_northing;

    x0 = -x0;
    y0 = -y0;

    /* Inverse projection initialisation */
    inv_proj_data.false_easting  = x0;
    inv_proj_data.false_northing = y0;
    inv_proj_data.centre_lon     = centralLongitude;
    inv_proj_data.centre_lat     = centralLatitude;
    if (centralLatitude < 0)
        inv_proj_data.sign = -1.0;
    else
        inv_proj_data.sign = +1.0;
    inv_proj_data.ind = 0;
    if (fabs(fabs(centralLatitude) - PI_OVER_2) > EPSILON) {
        inv_proj_data.ind = 1;
        con1              = inv_proj_data.sign * inv_proj_data.centre_lat;
        inv_proj_data.mcs = cos(con1);
        inv_proj_data.tcs = tan(0.5 * (PI_OVER_2 - con1));
    }
    lats_ = (double*)grib_context_malloc(h->context, nv_ * sizeof(double));
    if (!lats_) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, nv_ * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }
    lons_ = (double*)grib_context_malloc(h->context, nv_ * sizeof(double));
    if (!lons_) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", ITER, nv_ * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }
    lats = lats_;
    lons = lons_;
    /* These will be processed later in transform_iterator_data() */
    /* Dx = iScansNegatively == 0 ? Dx : -Dx; */
    /* Dy = jScansPositively == 1 ? Dy : -Dy; */

    y = 0;
    for (j = 0; j < ny; j++) {
        x = 0;
        for (i = 0; i < nx; i++) {
            /* Inverse projection from x,y to lat,lon */
            /* int index =i+j*nx; */
            double _x = (x - inv_proj_data.false_easting) * inv_proj_data.sign;
            double _y = (y - inv_proj_data.false_northing) * inv_proj_data.sign;
            double rh = sqrt(_x * _x + _y * _y);
            if (inv_proj_data.ind)
                ts = rh * inv_proj_data.tcs / (radius * inv_proj_data.mcs);
            else
                ts = rh / (radius * 2.0);
            *lats = inv_proj_data.sign * (PI_OVER_2 - 2 * atan(ts));
            if (rh == 0) {
                *lons = inv_proj_data.sign * inv_proj_data.centre_lon;
            }
            else {
                double temp = atan2(_x, -_y);
                *lons       = inv_proj_data.sign * temp + inv_proj_data.centre_lon;
            }
            *lats = *lats * RAD2DEG;
            *lons = *lons * RAD2DEG;
            while (*lons < 0)
                *lons += 360;
            while (*lons > 360)
                *lons -= 360;
            lons++;
            lats++;

            x += Dx;
        }
        y += Dy;
    }

    //     /*standardParallel = (southPoleOnPlane == 1) ? -90 : +90;*/
    //     if (jPointsAreConsecutive)
    //     {
    //         x=xFirst;
    //         for (i=0;i<nx;i++) {
    //             y=yFirst;
    //             for (j=0;j<ny;j++) {
    //                 rho=sqrt(x*x+y*y);
    //                 if (rho == 0) {
    //                     /* indeterminate case */
    //                     *lats = standardParallel;
    //                     *lons = centralLongitude;
    //                 }
    //                 else {
    //                     c=2*atan2(rho,(2.0*radius));
    //                     cosc=cos(c);
    //                     sinc=sin(c);
    //                     *lats = asin( cosc*sinphi1 + y*sinc*cosphi1/rho ) * RAD2DEG;
    //                     *lons = (lambda0+atan2(x*sinc, rho*cosphi1*cosc - y*sinphi1*sinc)) * RAD2DEG;
    //                 }
    //                 while (*lons<0)   *lons += 360;
    //                 while (*lons>360) *lons -= 360;
    //                 lons++;
    //                 lats++;
    //                 y+=Dy;
    //             }
    //             x+=Dx;
    //         }
    //     }
    //     else
    //     {
    //         y=yFirst;
    //         for (j=0;j<ny;j++) {
    //             x=xFirst;
    //             for (i=0;i<nx;i++) {
    //                 /* int index =i+j*nx; */
    //                 rho=sqrt(x*x+y*y);
    //                 if (rho == 0) {
    //                     /* indeterminate case */
    //                     *lats = standardParallel;
    //                     *lons = centralLongitude;
    //                 }
    //                 else {
    //                     c=2*atan2(rho,(2.0*radius));
    //                     cosc=cos(c);
    //                     sinc=sin(c);
    //                     *lats = asin( cosc*sinphi1 + y*sinc*cosphi1/rho ) * RAD2DEG;
    //                     *lons = (lambda0+atan2(x*sinc, rho*cosphi1*cosc - y*sinphi1*sinc)) * RAD2DEG;
    //                 }
    //                 while (*lons<0)   *lons += 360;
    //                 while (*lons>360) *lons -= 360;
    //                 lons++;
    //                 lats++;
    //                 x+=Dx;
    //             }
    //             y+=Dy;
    //         }
    //     }

    e_ = -1;

    /* Apply the scanning mode flags which may require data array to be transformed */
    ret = transform_iterator_data(h->context, data_,
                                  iScansNegatively, jScansPositively, jPointsAreConsecutive, alternativeRowScanning,
                                  nv_, nx, ny);

    return ret;
}

int PolarStereographic::destroy()
{
    DEBUG_ASSERT(h_);
    const grib_context* c = h_->context;
    grib_context_free(c, lats_);
    grib_context_free(c, lons_);

    return Gen::destroy();
}

}  // namespace eccodes::geo_iterator
