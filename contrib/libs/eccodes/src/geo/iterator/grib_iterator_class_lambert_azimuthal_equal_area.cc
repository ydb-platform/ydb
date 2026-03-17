/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_lambert_azimuthal_equal_area.h"

eccodes::geo_iterator::LambertAzimuthalEqualArea _grib_iterator_lambert_azimuthal_equal_area{};
eccodes::geo_iterator::Iterator* grib_iterator_lambert_azimuthal_equal_area = &_grib_iterator_lambert_azimuthal_equal_area;

namespace eccodes::geo_iterator {

#define ITER "Lambert azimuthal equal area Geoiterator"

int LambertAzimuthalEqualArea::next(double* lat, double* lon, double* val) const
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

#define P00 .33333333333333333333 /*   1 /     3 */
#define P01 .17222222222222222222 /*  31 /   180 */
#define P02 .10257936507936507937 /* 517 /  5040 */
#define P10 .06388888888888888888 /*  23 /   360 */
#define P11 .06640211640211640212 /* 251 /  3780 */
#define P20 .01677689594356261023 /* 761 / 45360 */

static void pj_authset(double es, double* APA)
{
    double t;
    APA[0] = es * P00;
    t      = es * es;
    APA[0] += t * P01;
    APA[1] = t * P10;
    t *= es;
    APA[0] += t * P02;
    APA[1] += t * P11;
    APA[2] = t * P20;
}
static double pj_authlat(double beta, const double* APA)
{
    double t = beta + beta;
    return (beta + APA[0] * sin(t) + APA[1] * sin(t + t) + APA[2] * sin(t + t + t));
}

static double pj_qsfn(double sinphi, double e, double one_es)
{
    double con, div1, div2;
    const double EPSILON = 1.0e-7;

    if (e >= EPSILON) {
        con  = e * sinphi;
        div1 = 1.0 - con * con;
        div2 = 1.0 + con;

        /* avoid zero division, fail gracefully */
        if (div1 == 0.0 || div2 == 0.0)
            return HUGE_VAL;

        return (one_es * (sinphi / div1 - (.5 / e) * log((1. - con) / div2)));
    }
    else
        return (sinphi + sinphi);
}

#define EPS10 1.e-10
int LambertAzimuthalEqualArea::init_oblate(grib_handle* h,
                       size_t nv, long nx, long ny,
                       double Dx, double Dy, double earthMinorAxisInMetres, double earthMajorAxisInMetres,
                       double latFirstInRadians, double lonFirstInRadians,
                       double centralLongitudeInRadians, double standardParallelInRadians,
                       long iScansNegatively, long jScansPositively, long jPointsAreConsecutive)
{
    double *lats, *lons;
    long i, j;
    double x0, y0, x, y;
    double coslam, sinlam, sinphi, sinphi_, q, sinb = 0.0, cosb = 0.0, b = 0.0, cosb2;
    double Q__qp = 0, Q__rq = 0, Q__cosb1, Q__sinb1, Q__dd, Q__xmf, Q__ymf, t;
    /* double Q__mmf = 0; */
    double e, es, temp, one_es;
    double APA[3] = {
        0,
    };
    double xFirst, yFirst;

    Dx = iScansNegatively == 0 ? Dx / 1000 : -Dx / 1000;
    Dy = jScansPositively == 1 ? Dy / 1000 : -Dy / 1000;

    temp   = (earthMajorAxisInMetres - earthMinorAxisInMetres) / earthMajorAxisInMetres;
    es     = 2 * temp - temp * temp;
    one_es = 1.0 - es;
    e      = sqrt(es);

    coslam = cos(lonFirstInRadians - centralLongitudeInRadians); /* cos(lp.lam) */
    sinlam = sin(lonFirstInRadians - centralLongitudeInRadians);
    sinphi = sin(latFirstInRadians); /* sin(lp.phi) */
    q      = pj_qsfn(sinphi, e, one_es);

    t = fabs(standardParallelInRadians);
    if (t > M_PI_2 + EPS10) {
        return GRIB_GEOCALCULUS_PROBLEM;
    }
    /* if (fabs(t - M_HALFPI) < EPS10)
        Q->mode = P->phi0 < 0. ? S_POLE : N_POLE;
       else if (fabs(t) < EPS10)
        Q->mode = EQUIT;
       else
        Q->mode = OBLIQ;
    */
    Q__qp = pj_qsfn(1.0, e, one_es);
    /* Q__mmf = 0.5 / one_es;  ----  TODO(masn): do I need this? */
    pj_authset(es, APA); /* sets up APA array */
    Q__rq    = sqrt(0.5 * Q__qp);
    sinphi_  = sin(standardParallelInRadians); /*  (P->phi0); */
    Q__sinb1 = pj_qsfn(sinphi_, e, one_es) / Q__qp;
    Q__cosb1 = sqrt(1.0 - Q__sinb1 * Q__sinb1);
    if (Q__cosb1 == 0) {
        Q__dd = 1.0;
    }
    else {
        Q__dd = cos(standardParallelInRadians) / (sqrt(1. - es * sinphi_ * sinphi_) * Q__rq * Q__cosb1);
    }
    Q__ymf = (Q__xmf = Q__rq) / Q__dd;
    Q__xmf *= Q__dd;

    sinb  = q / Q__qp;
    cosb2 = 1.0 - sinb * sinb;
    cosb  = cosb2 > 0 ? sqrt(cosb2) : 0;
    b     = 1. + Q__sinb1 * sinb + Q__cosb1 * cosb * coslam;
    if (fabs(b) < EPS10) {
        return GRIB_GEOCALCULUS_PROBLEM;
    }
    b = sqrt(2.0 / b);

    /* OBLIQUE */
    y0 = Q__ymf * b * (Q__cosb1 * sinb - Q__sinb1 * cosb * coslam);
    x0 = Q__xmf * b * cosb * sinlam;

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
    lats = lats_;
    lons = lons_;

    /* Populate the lat and lon arrays */
    {
        xFirst = x0;
        yFirst = y0;
        y      = yFirst;
        for (j = 0; j < ny; j++) {
            x = xFirst;
            for (i = 0; i < nx; i++) {
                double cCe, sCe, rho, ab = 0.0, lp__lam, lp__phi, xy_x = x, xy_y = y;
                xy_x /= Q__dd;
                xy_y *= Q__dd;
                rho = hypot(xy_x, xy_y);
                ECCODES_ASSERT(rho >= EPS10); /* TODO(masn): check */
                const double asin_arg = (0.5 * rho / Q__rq);
                if (asin_arg < -1.0 || asin_arg > 1.0) {
                    grib_context_log(h->context, GRIB_LOG_ERROR, "Invalid value: arcsin argument=%g", asin_arg);
                    return GRIB_GEOCALCULUS_PROBLEM;
                }
                sCe = 2. * asin(asin_arg);
                cCe = cos(sCe);
                sCe = sin(sCe);
                xy_x *= sCe;
                /* if oblique */
                ab   = cCe * Q__sinb1 + xy_y * sCe * Q__cosb1 / rho;
                xy_y = rho * Q__cosb1 * cCe - xy_y * Q__sinb1 * sCe;
                /*  else
                    ab = xy.y * sCe / rho;
                    xy.y = rho * cCe;
                */
                lp__lam = atan2(xy_x, xy_y);         /* longitude */
                lp__phi = pj_authlat(asin(ab), APA); /* latitude */

                *lats = lp__phi * RAD2DEG;
                *lons = (lp__lam + centralLongitudeInRadians) * RAD2DEG;

                lons++;
                lats++;

                x += Dx / earthMajorAxisInMetres;
            }
            y += Dy / earthMajorAxisInMetres;
        }
    }

    return GRIB_SUCCESS;
}

int LambertAzimuthalEqualArea::init_sphere(grib_handle* h,
                       size_t nv, long nx, long ny,
                       double Dx, double Dy, double radius,
                       double latFirstInRadians, double lonFirstInRadians,
                       double centralLongitudeInRadians, double standardParallelInRadians,
                       long iScansNegatively, long jScansPositively, long jPointsAreConsecutive)
{
    double *lats, *lons;
    double phi1, lambda0, xFirst, yFirst, x, y;
    double kp, sinphi1, cosphi1;
    double sinphi, cosphi, cosdlambda, sindlambda;
    double cosc, sinc;
    long i, j;
    double c, rho;
    const double epsilon = 1.0e-20;
    const double d2r     = acos(0.0) / 90.0;

    lambda0 = centralLongitudeInRadians;
    phi1    = standardParallelInRadians;

    cosphi1 = cos(phi1);
    sinphi1 = sin(phi1);

    Dx          = iScansNegatively == 0 ? Dx / 1000 : -Dx / 1000;
    Dy          = jScansPositively == 1 ? Dy / 1000 : -Dy / 1000;
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
    lats = lats_;
    lons = lons_;

    /* compute xFirst,yFirst in metres */
    sinphi     = sin(latFirstInRadians);
    cosphi     = cos(latFirstInRadians);
    cosdlambda = cos(lonFirstInRadians - lambda0);
    sindlambda = sin(lonFirstInRadians - lambda0);
    kp         = radius * sqrt(2.0 / (1 + sinphi1 * sinphi + cosphi1 * cosphi * cosdlambda));
    xFirst     = kp * cosphi * sindlambda;
    yFirst     = kp * (cosphi1 * sinphi - sinphi1 * cosphi * cosdlambda);

    if (jPointsAreConsecutive) {
        x = xFirst;
        for (i = 0; i < nx; i++) {
            double xsq = x * x;
            y          = yFirst;
            for (j = 0; j < ny; j++) {
                rho = sqrt(xsq + y * y);
                if (rho > epsilon) {
                    c     = 2 * asin(rho / (2.0 * radius));
                    cosc  = cos(c);
                    sinc  = sin(c);
                    *lats = asin(cosc * sinphi1 + y * sinc * cosphi1 / rho) / d2r;
                    *lons = (lambda0 + atan2(x * sinc, rho * cosphi1 * cosc - y * sinphi1 * sinc)) / d2r;
                }
                else {
                    *lats = phi1 / d2r;
                    *lons = lambda0 / d2r;
                }
                if (*lons < 0)
                    *lons += 360;
                lons++;
                lats++;

                y += Dy;
            }
            x += Dx;
        }
    }
    else {
        y = yFirst;
        for (j = 0; j < ny; j++) {
            double ysq = y * y;
            x          = xFirst;
            for (i = 0; i < nx; i++) {
                rho = sqrt(x * x + ysq);
                if (rho > epsilon) {
                    c     = 2 * asin(rho / (2.0 * radius));
                    cosc  = cos(c);
                    sinc  = sin(c);
                    *lats = asin(cosc * sinphi1 + y * sinc * cosphi1 / rho) / d2r;
                    *lons = (lambda0 + atan2(x * sinc, rho * cosphi1 * cosc - y * sinphi1 * sinc)) / d2r;
                }
                else {
                    *lats = phi1 / d2r;
                    *lons = lambda0 / d2r;
                }
                if (*lons < 0)
                    *lons += 360;
                lons++;
                lats++;

                x += Dx;
            }
            y += Dy;
        }
    }
    return GRIB_SUCCESS;
}

int LambertAzimuthalEqualArea::init(grib_handle* h, grib_arguments* args)
{
    int err = 0;
    if ((err = Gen::init(h, args)) != GRIB_SUCCESS)
        return err;

    int is_oblate = 0;
    double lonFirstInDegrees, latFirstInDegrees, lonFirstInRadians, latFirstInRadians, radius = 0;
    long nx, ny;
    double standardParallelInDegrees, centralLongitudeInDegrees;
    double standardParallelInRadians, centralLongitudeInRadians;
    double Dx, Dy;
    long alternativeRowScanning, iScansNegatively;
    long jScansPositively, jPointsAreConsecutive;
    double earthMajorAxisInMetres = 0, earthMinorAxisInMetres = 0;

    const char* sradius                 = args->get_name(h, carg_++);
    const char* snx                     = args->get_name(h, carg_++);
    const char* sny                     = args->get_name(h, carg_++);
    const char* slatFirstInDegrees      = args->get_name(h, carg_++);
    const char* slonFirstInDegrees      = args->get_name(h, carg_++);
    const char* sstandardParallel       = args->get_name(h, carg_++);
    const char* scentralLongitude       = args->get_name(h, carg_++);
    const char* sDx                     = args->get_name(h, carg_++);
    const char* sDy                     = args->get_name(h, carg_++);
    const char* siScansNegatively       = args->get_name(h, carg_++);
    const char* sjScansPositively       = args->get_name(h, carg_++);
    const char* sjPointsAreConsecutive  = args->get_name(h, carg_++);
    const char* salternativeRowScanning = args->get_name(h, carg_++);
    const double d2r                    = acos(0.0) / 90.0;

    is_oblate = grib_is_earth_oblate(h);
    if (is_oblate) {
        if ((err = grib_get_double_internal(h, "earthMinorAxisInMetres", &earthMinorAxisInMetres)) != GRIB_SUCCESS) return err;
        if ((err = grib_get_double_internal(h, "earthMajorAxisInMetres", &earthMajorAxisInMetres)) != GRIB_SUCCESS) return err;
    }
    else {
        if ((err = grib_get_double_internal(h, sradius, &radius)) != GRIB_SUCCESS) return err;
    }

    if ((err = grib_get_long_internal(h, snx, &nx)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, sny, &ny)) != GRIB_SUCCESS)
        return err;

    if (nv_ != nx * ny) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Wrong number of points (%zu!=%ldx%ld)", ITER, nv_, nx, ny);
        return GRIB_WRONG_GRID;
    }
    if ((err = grib_get_double_internal(h, slatFirstInDegrees, &latFirstInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, slonFirstInDegrees, &lonFirstInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, sstandardParallel, &standardParallelInDegrees)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(h, scentralLongitude, &centralLongitudeInDegrees)) != GRIB_SUCCESS)
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

    latFirstInRadians         = latFirstInDegrees * d2r;
    lonFirstInRadians         = lonFirstInDegrees * d2r;
    centralLongitudeInRadians = centralLongitudeInDegrees * d2r;
    standardParallelInRadians = standardParallelInDegrees * d2r;

    if (is_oblate) {
        err = init_oblate(h, nv_, nx, ny,
                          Dx, Dy, earthMinorAxisInMetres, earthMajorAxisInMetres,
                          latFirstInRadians, lonFirstInRadians,
                          centralLongitudeInRadians, standardParallelInRadians,
                          iScansNegatively, jScansPositively, jPointsAreConsecutive);
    }
    else {
        err = init_sphere(h, nv_, nx, ny,
                          Dx, Dy, radius,
                          latFirstInRadians, lonFirstInRadians,
                          centralLongitudeInRadians, standardParallelInRadians,
                          iScansNegatively, jScansPositively, jPointsAreConsecutive);
    }
    if (err) return err;

    e_ = -1;

    return GRIB_SUCCESS;
}

int LambertAzimuthalEqualArea::destroy()
{
    DEBUG_ASSERT(h_);
    const grib_context* c = h_->context;
    grib_context_free(c, lats_);
    grib_context_free(c, lons_);

    return Gen::destroy();
}

}  // namespace eccodes::geo_iterator
