/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2latlon.h"

grib_accessor_g2latlon_t _grib_accessor_g2latlon{};
grib_accessor* grib_accessor_g2latlon = &_grib_accessor_g2latlon;

void grib_accessor_g2latlon_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n = 0;

    grid_  = c->get_name(grib_handle_of_accessor(this), n++);
    index_ = c->get_long(grib_handle_of_accessor(this), n++);
    given_ = c->get_name(grib_handle_of_accessor(this), n++);
}

int grib_accessor_g2latlon_t::unpack_double(double* val, size_t* len)
{
    int ret = 0;

    long given = 1;
    double grid[6];
    size_t size = 6;

    if (*len < 1) {
        ret = GRIB_ARRAY_TOO_SMALL;
        return ret;
    }

    if (given_)
        if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), given_, &given)) != GRIB_SUCCESS)
            return ret;

    if (!given) {
        *val = GRIB_MISSING_DOUBLE;
        return GRIB_SUCCESS;
    }

    if ((ret = grib_get_double_array_internal(grib_handle_of_accessor(this), grid_, grid, &size)) != GRIB_SUCCESS)
        return ret;

    *val = grid[index_];

    return GRIB_SUCCESS;
}

int grib_accessor_g2latlon_t::pack_double(const double* val, size_t* len)
{
    int ret = 0;
    double grid[6];
    size_t size       = 6;
    double new_val    = *val;
    grib_handle* hand = grib_handle_of_accessor(this);

    if (*len < 1) {
        ret = GRIB_ARRAY_TOO_SMALL;
        return ret;
    }

    if (given_) {
        long given = *val != GRIB_MISSING_DOUBLE;
        if ((ret = grib_set_long_internal(hand, given_, given)) != GRIB_SUCCESS)
            return ret;
    }

    if ((ret = grib_get_double_array_internal(hand, grid_, grid, &size)) != GRIB_SUCCESS)
        return ret;

    /* index 1 is longitudeOfFirstGridPointInDegrees
     * index 3 is longitudeOfLastGridPointInDegrees
     */
    if ((index_ == 1 || index_ == 3)) {
        /* WMO regulation for GRIB edition 2:
         * The longitude values shall be limited to the range 0 to 360 degrees inclusive */
        new_val = normalise_longitude_in_degrees(*val);
        if (hand->context->debug && new_val != *val) {
            fprintf(stderr, "ECCODES DEBUG pack_double g2latlon: normalise longitude %g -> %g\n", *val, new_val);
        }
    }
    grid[index_] = new_val;

    return grib_set_double_array_internal(hand, grid_, grid, size);
}

int grib_accessor_g2latlon_t::pack_missing()
{
    double missing = GRIB_MISSING_DOUBLE;
    size_t size    = 1;

    if (!given_)
        return GRIB_NOT_IMPLEMENTED;

    return pack_double(&missing, &size);
}

int grib_accessor_g2latlon_t::is_missing()
{
    long given = 1;

    if (given_)
        grib_get_long_internal(grib_handle_of_accessor(this), given_, &given);

    return !given;
}
