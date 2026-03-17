/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_latlon_increment.h"

grib_accessor_latlon_increment_t _grib_accessor_latlon_increment{};
grib_accessor* grib_accessor_latlon_increment = &_grib_accessor_latlon_increment;

void grib_accessor_latlon_increment_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n             = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    directionIncrementGiven_ = c->get_name(hand, n++);
    directionIncrement_      = c->get_name(hand, n++);
    scansPositively_         = c->get_name(hand, n++);
    first_                   = c->get_name(hand, n++);
    last_                    = c->get_name(hand, n++);
    numberOfPoints_          = c->get_name(hand, n++);
    angleMultiplier_         = c->get_name(hand, n++);
    angleDivisor_            = c->get_name(hand, n++);
    isLongitude_             = c->get_long(hand, n++);
}

int grib_accessor_latlon_increment_t::unpack_double(double* val, size_t* len)
{
    int ret           = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    long directionIncrementGiven = 0;
    long directionIncrement      = 0;
    long angleDivisor            = 1;
    long angleMultiplier         = 1;
    double first                 = 0;
    double last                  = 0;
    long numberOfPoints          = 0;
    long scansPositively         = 0;

    if (*len < 1)
        return GRIB_ARRAY_TOO_SMALL;

    if ((ret = grib_get_long_internal(hand, directionIncrementGiven_, &directionIncrementGiven)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, scansPositively_, &scansPositively)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, directionIncrement_, &directionIncrement)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(hand, first_, &first)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(hand, last_, &last)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, numberOfPoints_, &numberOfPoints)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, angleMultiplier_, &angleMultiplier)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, angleDivisor_, &angleDivisor)) != GRIB_SUCCESS)
        return ret;

    if (isLongitude_) {
        if (last < first && scansPositively)
            last += 360;
        /*if (last > first && !scansPositively) first-=360;*/
    }

    if (!directionIncrementGiven && numberOfPoints != GRIB_MISSING_LONG) {
        if (numberOfPoints < 2) {
            /* We cannot compute the increment if we don't have enough points! */
            grib_context_log(parent_->h->context, GRIB_LOG_ERROR,
                             "Cannot compute lat/lon increments. Not enough points!");
            return GRIB_GEOCALCULUS_PROBLEM;
        }
        if (!scansPositively) { /* scans negatively */
            if (first > last) {
                *val = (first - last) / (numberOfPoints - 1);
            }
            else {
                *val = (first + 360.0 - last) / (numberOfPoints - 1);
            }
        }
        else {
            /* scans positively */
            if (last > first) {
                *val = (last - first) / (numberOfPoints - 1);
            }
            else {
                *val = (last + 360.0 - first) / (numberOfPoints - 1);
            }
        }
    }
    else if (numberOfPoints == GRIB_MISSING_LONG) {
        *val = GRIB_MISSING_DOUBLE;
    }
    else {
        ECCODES_ASSERT(angleDivisor != 0);
        *val = (double)directionIncrement / angleDivisor * angleMultiplier;
    }

    if (ret == GRIB_SUCCESS)
        *len = 1;

    return ret;
}

int grib_accessor_latlon_increment_t::pack_double(const double* val, size_t* len)
{
    int ret                  = 0;
    long codedNumberOfPoints = 0;
    grib_handle* hand        = grib_handle_of_accessor(this);

    long directionIncrementGiven = 0;
    long directionIncrement      = 0;
    long angleDivisor            = 1;
    long angleMultiplier         = 1;
    double first                 = 0;
    double last                  = 0;
    long numberOfPoints          = 0;
    /* long numberOfPointsInternal = 0; */
    long scansPositively            = 0;
    double directionIncrementDouble = 0;

    ret = grib_get_double_internal(hand, first_, &first);
    if (ret != GRIB_SUCCESS)
        return ret;

    ret = grib_get_double_internal(hand, last_, &last);
    if (ret != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, directionIncrementGiven_, &directionIncrementGiven)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, numberOfPoints_, &numberOfPoints)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, scansPositively_, &scansPositively)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, angleMultiplier_, &angleMultiplier)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, angleDivisor_, &angleDivisor)) != GRIB_SUCCESS)
        return ret;

    if (isLongitude_) {
        if (last < first && scansPositively)
            last += 360;
        if (last > first && !scansPositively)
            first -= 360;
    }

    if (*val == GRIB_MISSING_DOUBLE) {
        directionIncrement      = GRIB_MISSING_LONG;
        directionIncrementGiven = 1;
        numberOfPoints          = GRIB_MISSING_LONG;
    }
    else {
        /* numberOfPointsInternal = 1+rint(fabs((last-first) / *val)); */

        directionIncrementDouble = rint(*val * (double)angleDivisor / (double)angleMultiplier);

        directionIncrement = (long)directionIncrementDouble;
        if (directionIncrement == 0) {
            directionIncrement      = GRIB_MISSING_LONG;
            directionIncrementGiven = 0;
        }
    }

    // ret = grib_set_long_internal(hand, numberOfPoints_ ,numberOfPoints);
    // if(ret) grib_context_log(a->context, GRIB_LOG_ERROR, "Accessor %s cannot pack value for %s error %d \n", a->name, numberOfPoints_ , ret);

    grib_get_long_internal(hand, numberOfPoints_, &codedNumberOfPoints);

    ret = grib_set_long_internal(hand, directionIncrement_, directionIncrement);
    if (ret)
        return ret;

    ret = grib_set_long_internal(hand, directionIncrementGiven_, directionIncrementGiven);
    if (ret)
        return ret;

    if (ret == GRIB_SUCCESS)
        *len = 1;

    return GRIB_SUCCESS;
}

int grib_accessor_latlon_increment_t::is_missing()
{
    size_t len = 1;
    double val = 0;

    unpack_double(&val, &len);

    return (val == GRIB_MISSING_DOUBLE);
}
