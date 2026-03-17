/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_scale.h"

grib_accessor_scale_t _grib_accessor_scale{};
grib_accessor* grib_accessor_scale = &_grib_accessor_scale;

void grib_accessor_scale_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n = 0;

    value_      = c->get_name(grib_handle_of_accessor(this), n++);
    multiplier_ = c->get_name(grib_handle_of_accessor(this), n++);
    divisor_    = c->get_name(grib_handle_of_accessor(this), n++);
    truncating_ = c->get_name(grib_handle_of_accessor(this), n++);
}

int grib_accessor_scale_t::unpack_double(double* val, size_t* len)
{
    int ret         = 0;
    long value      = 0;
    long multiplier = 0;
    long divisor    = 0;

    if (*len < 1) {
        ret = GRIB_ARRAY_TOO_SMALL;
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Accessor %s cannot gather value for %s and/or %s",
                         name_, multiplier, divisor_);
        return ret;
    }

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), divisor_, &divisor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), multiplier_, &multiplier)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), value_, &value)) != GRIB_SUCCESS)
        return ret;

    if (value == GRIB_MISSING_LONG)
        *val = GRIB_MISSING_DOUBLE;
    else
        *val = ((double)(value * multiplier)) / divisor;

    if (ret == GRIB_SUCCESS)
        *len = 1;

    return ret;
}

int grib_accessor_scale_t::pack_long(const long* val, size_t* len)
{
    const double dval = (double)*val;
    return pack_double(&dval, len);
}

int grib_accessor_scale_t::pack_double(const double* val, size_t* len)
{
    int ret = 0;

    long value      = 0;
    long divisor    = 0;
    long multiplier = 0;
    long truncating = 0;
    double x;

    ret = grib_get_long_internal(grib_handle_of_accessor(this), divisor_, &divisor);
    if (ret != GRIB_SUCCESS) return ret;

    ret = grib_get_long_internal(grib_handle_of_accessor(this), multiplier_, &multiplier);
    if (ret != GRIB_SUCCESS) return ret;

    if (truncating_) {
        ret = grib_get_long_internal(grib_handle_of_accessor(this), truncating_, &truncating);
        if (ret != GRIB_SUCCESS) return ret;
    }

    if (multiplier == 0) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Accessor %s: cannot divide by a zero multiplier %s",
                         name_, multiplier_);
        return GRIB_ENCODING_ERROR;
    }

    x = *val * (double)divisor / (double)multiplier;
    if (*val == GRIB_MISSING_DOUBLE)
        value = GRIB_MISSING_LONG;
    else if (truncating) {
        value = (long)x;
    }
    else {
        value = x > 0 ? (long)(x + 0.5) : (long)(x - 0.5);
    }

    ret = grib_set_long_internal(grib_handle_of_accessor(this), value_, value);
    if (ret)
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Accessor %s: cannot pack value for %s (%s)\n",
                         name_, value_, grib_get_error_message(ret));

    if (ret == GRIB_SUCCESS)
        *len = 1;

    return ret;
}

int grib_accessor_scale_t::is_missing()
{
    grib_accessor* av = grib_find_accessor(grib_handle_of_accessor(this), value_);

    if (!av)
        return GRIB_NOT_FOUND;
    return av->is_missing_internal();
    //     int ret=0;
    //     long value=0;
    //     if((ret = grib_get_long_internal(grib_handle_of_accessor(this),value_ , &value))!= GRIB_SUCCESS){
    //         grib_context_log(context_ , GRIB_LOG_ERROR,
    //         "Accessor %s cannot gather value for %s error %d \n", name_ ,
    //         value_ , ret);
    //         return 0;
    //     }
    //     return (value == GRIB_MISSING_LONG);
}
