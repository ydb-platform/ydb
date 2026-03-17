/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2grid.h"

grib_accessor_g2grid_t _grib_accessor_g2grid{};
grib_accessor* grib_accessor_g2grid = &_grib_accessor_g2grid;

void grib_accessor_g2grid_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);
    int n             = 0;

    latitude_first_  = c->get_name(hand, n++);
    longitude_first_ = c->get_name(hand, n++);
    latitude_last_   = c->get_name(hand, n++);
    longitude_last_  = c->get_name(hand, n++);
    i_increment_     = c->get_name(hand, n++);
    j_increment_     = c->get_name(hand, n++);
    basic_angle_     = c->get_name(hand, n++);
    sub_division_    = c->get_name(hand, n++);

    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC | GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_g2grid_t::value_count(long* count)
{
    *count = 6;
    return 0;
}

// GRIB edition 2 uses microdegrees
#define ANGLE_SUBDIVISIONS (1000 * 1000)

int grib_accessor_g2grid_t::unpack_double(double* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    int ret           = 0;

    long basic_angle  = 0;
    long sub_division = 0;
    int n             = 0;
    long v[6];

    if (*len < 6) {
        ret = GRIB_ARRAY_TOO_SMALL;
        return ret;
    }

    if ((ret = grib_get_long_internal(hand, basic_angle_, &basic_angle)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, sub_division_, &sub_division)) != GRIB_SUCCESS)
        return ret;

    if (sub_division == GRIB_MISSING_LONG || sub_division == 0)
        sub_division = ANGLE_SUBDIVISIONS;

    if (basic_angle == 0)
        basic_angle = 1;

    n = 0;
    if ((ret = grib_get_long_internal(hand, latitude_first_, &v[n++])) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, longitude_first_, &v[n++])) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, latitude_last_, &v[n++])) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, longitude_last_, &v[n++])) != GRIB_SUCCESS)
        return ret;

    if (!i_increment_)
        v[n++] = GRIB_MISSING_LONG;
    else if ((ret = grib_get_long_internal(hand, i_increment_, &v[n++])) != GRIB_SUCCESS)
        return ret;

    if (!j_increment_) {
        v[n++] = GRIB_MISSING_LONG;
    }
    else {
        if ((ret = grib_get_long_internal(hand, j_increment_, &v[n++])) != GRIB_SUCCESS)
            return ret;
    }
    for (int i = 0; i < n; i++)
        if (v[i] == GRIB_MISSING_LONG)
            val[i] = GRIB_MISSING_DOUBLE;
        else
            val[i] = (double)v[i] / (double)sub_division * (double)basic_angle;

    return GRIB_SUCCESS;
}

static long gcd(long a, long b)
{
    if (b > a)
        return gcd(b, a);
    if (b == 0)
        return a;
    return gcd(b, a % b);
}

static long lcm(long a, long b)
{
    return a * b / gcd(a, b);
}

static int is_ok(const double* val, long v[6], double basic_angle, double sub_division)
{
    int i;
    int ok = 1;

    for (i = 0; i < 6; i++) {
        if (val[i] == GRIB_MISSING_DOUBLE)
            v[i] = GRIB_MISSING_LONG;
        else {
            double d = (val[i] * sub_division) / basic_angle;
            double e;
            d    = round(d); /* GRIB-941 */
            v[i] = d;
            e    = (v[i] * basic_angle) / sub_division;

            /* if(fabs(e - val[i]) >= 1e-6) */
            if (fabs(e - val[i]) > 0) {
                /* printf("e=%g val=%g diff=%g\n",e,val[i],e-val[i]); */
                ok = 0;
            }
        }
    }
    return ok;
}

static int trial(const double* val, long v[6], long* basic_angle, long* sub_division)
{
    int i = 0;
    long ni, nj;

    for (i = 0; i < 6; i++)
        if (val[i] == GRIB_MISSING_DOUBLE)
            return 0;

    if (val[4] == 0)
        return 0;
    if (val[5] == 0)
        return 0;

    ni = (long)(0.5 + fabs((val[0] - val[2]) / val[4])) + 1;
    nj = (long)(0.5 + fabs((val[1] - val[3]) / val[5])) + 1;

    *basic_angle  = 360;
    *sub_division = lcm(ni, nj);

    // printf("ni = %ld, nj = %ld , basic_angle=%ld sub_division = %ld\n",
    //        ni, nj, *basic_angle,*sub_division);

    if (*sub_division < 0)
        return 0;

    ECCODES_ASSERT(*sub_division >= 0);

    return is_ok(val, v, *basic_angle, *sub_division);
}

int grib_accessor_g2grid_t::pack_double(const double* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    int ret;
    long v[6];
    int n;
    long basic_angle;
    long sub_division;

    if (*len < 6) {
        return GRIB_ARRAY_TOO_SMALL;
    }

    /* printf("pack_double %g %g %g %g %g %g\n",val[0],val[1],val[2],val[3],val[4],val[5]);*/

    if (is_ok(val, v, 1, ANGLE_SUBDIVISIONS)) {
        basic_angle  = 1;
        sub_division = ANGLE_SUBDIVISIONS;
    }
    else if (trial(val, v, &basic_angle, &sub_division)) {
    }
    else {
        basic_angle  = 1;
        sub_division = ANGLE_SUBDIVISIONS;

        if (!is_ok(val, v, basic_angle, sub_division))
            grib_context_log(context_, GRIB_LOG_DEBUG, "Grid cannot be coded with any loss of precision");
    }

    if (basic_angle == 1 && sub_division == ANGLE_SUBDIVISIONS) {
        basic_angle  = 0;
        sub_division = GRIB_MISSING_LONG;
    }

    if ((ret = grib_set_long_internal(hand, basic_angle_, basic_angle)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_long_internal(hand, sub_division_, sub_division)) != GRIB_SUCCESS)
        return ret;

    n = 0;
    if ((ret = grib_set_long_internal(hand, latitude_first_, v[n++])) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_long_internal(hand, longitude_first_, v[n++])) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_long_internal(hand, latitude_last_, v[n++])) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_long_internal(hand, longitude_last_, v[n++])) != GRIB_SUCCESS)
        return ret;

    if (!i_increment_)
        n++;
    else if ((ret = grib_set_long_internal(hand, i_increment_, v[n++])) != GRIB_SUCCESS)
        return ret;

    if (!j_increment_)
        n++;
    else if ((ret = grib_set_long_internal(hand, j_increment_, v[n++])) != GRIB_SUCCESS)
        return ret;

    return GRIB_SUCCESS;
}
