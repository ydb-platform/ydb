/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_latlon.h"

eccodes::geo_iterator::Latlon _grib_iterator_latlon{};
eccodes::geo_iterator::Iterator* grib_iterator_latlon = &_grib_iterator_latlon;

namespace eccodes::geo_iterator {

int Latlon::next(double* lat, double* lon, double* val) const
{
    /* GRIB-238: Support rotated lat/lon grids */
    double ret_lat, ret_lon, ret_val = 0;
    if ((long)e_ >= (long)(nv_ - 1))
        return 0;

    e_++;

    /* Assumptions:
     *   All rows scan in the same direction (alternativeRowScanning==0)
     */
    if (!jPointsAreConsecutive_) {
        /* Adjacent points in i (x) direction are consecutive */
        ret_lat = lats_[(long)floor(e_ / Ni_)];
        ret_lon = lons_[(long)e_ % Ni_];
        if (data_)
            ret_val = data_[e_];
    }
    else {
        /* Adjacent points in j (y) direction is consecutive */
        ret_lon = lons_[(long)e_ / Nj_];
        ret_lat = lats_[(long)floor(e_ % Nj_)];
        if (data_)
            ret_val = data_[e_];
    }

    /* See ECC-808: Some users want to disable the unrotate */
    if (isRotated_ && !disableUnrotate_) {
        double new_lat = 0, new_lon = 0;
        unrotate(ret_lat, ret_lon,
                 angleOfRotation_, southPoleLat_, southPoleLon_,
                 &new_lat, &new_lon);
        ret_lat = new_lat;
        ret_lon = new_lon;
    }

    *lat = ret_lat;
    *lon = ret_lon;
    if (val && data_) {
        *val = ret_val;
    }
    return 1;
}

int Latlon::init(grib_handle* h, grib_arguments* args)
{
    int err = 0;
    if ((err = Regular::init(h, args)) != GRIB_SUCCESS)
        return err;

    double jdir;
    double lat1 = 0, lat2 = 0, north = 0, south = 0;
    long jScansPositively;
    long lai;

    const char* s_lat1            = args->get_name(h, carg_++);
    const char* s_jdir            = args->get_name(h, carg_++);
    const char* s_jScansPos       = args->get_name(h, carg_++);
    const char* s_jPtsConsec      = args->get_name(h, carg_++);
    const char* s_isRotatedGrid   = args->get_name(h, carg_++);
    const char* s_angleOfRotation = args->get_name(h, carg_++);
    const char* s_latSouthernPole = args->get_name(h, carg_++);
    const char* s_lonSouthernPole = args->get_name(h, carg_++);

    angleOfRotation_ = 0;
    isRotated_       = 0;
    southPoleLat_    = 0;
    southPoleLon_    = 0;
    disableUnrotate_ = 0; /* unrotate enabled by default */

    if ((err = grib_get_long(h, s_isRotatedGrid, &isRotated_)))
        return err;
    if (isRotated_) {
        if ((err = grib_get_double_internal(h, s_angleOfRotation, &angleOfRotation_)))
            return err;
        if ((err = grib_get_double_internal(h, s_latSouthernPole, &southPoleLat_)))
            return err;
        if ((err = grib_get_double_internal(h, s_lonSouthernPole, &southPoleLon_)))
            return err;
    }

    if ((err = grib_get_double_internal(h, s_lat1, &lat1)))
        return err;
    if ((err = grib_get_double_internal(h, "latitudeLastInDegrees", &lat2)))
        return err;
    if ((err = grib_get_double_internal(h, s_jdir, &jdir)))  // can be GRIB_MISSING_DOUBLE
        return err;
    if ((err = grib_get_long_internal(h, s_jScansPos, &jScansPositively)))
        return err;
    if ((err = grib_get_long_internal(h, s_jPtsConsec, &jPointsAreConsecutive_)))
        return err;
    if ((err = grib_get_long(h, "iteratorDisableUnrotate", &disableUnrotate_)))
        return err;

    /* ECC-984: If jDirectionIncrement is missing, then we cannot use it (See jDirectionIncrementGiven) */
    /* So try to compute the increment */
    if ((grib_is_missing(h, s_jdir, &err) && err == GRIB_SUCCESS) || (jdir == GRIB_MISSING_DOUBLE)) {
        const long Nj = Nj_;
        ECCODES_ASSERT(Nj > 1);
        if (lat1 > lat2) {
            jdir = (lat1 - lat2) / (Nj - 1);
        }
        else {
            jdir = (lat2 - lat1) / (Nj - 1);
        }
        grib_context_log(h->context, GRIB_LOG_DEBUG,
                         "Cannot use jDirectionIncrement. Using value of %.6f obtained from La1, La2 and Nj", jdir);
    }

    if (jScansPositively) {
        north = lat2;
        south = lat1;
        jdir  = -jdir;
    }
    else {
        north = lat1;
        south = lat2;
    }
    if (south > north) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Lat/Lon Geoiterator: First and last latitudes are inconsistent with scanning order: lat1=%g, lat2=%g jScansPositively=%ld",
                         lat1, lat2, jScansPositively);
        return GRIB_WRONG_GRID;
    }

    for (lai = 0; lai < Nj_; lai++) {
        lats_[lai] = lat1;
        lat1 -= jdir;
    }
    /* ECC-1406: Due to rounding, errors can accumulate.
     * So we ensure the last latitude is latitudeOfLastGridPointInDegrees
     */
    lats_[Nj_ - 1] = lat2;

    e_ = -1;
    return err;
}

}  // namespace eccodes::geo_iterator
