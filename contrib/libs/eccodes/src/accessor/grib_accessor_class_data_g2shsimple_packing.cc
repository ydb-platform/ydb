/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g2shsimple_packing.h"

grib_accessor_data_g2shsimple_packing_t _grib_accessor_data_g2shsimple_packing{};
grib_accessor* grib_accessor_data_g2shsimple_packing = &_grib_accessor_data_g2shsimple_packing;

void grib_accessor_data_g2shsimple_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_shsimple_packing_t::init(v, args);

    numberOfValues_     = args->get_name(grib_handle_of_accessor(this), 2);
    numberOfDataPoints_ = args->get_name(grib_handle_of_accessor(this), 3);
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_g2shsimple_packing_t::value_count(long* len)
{
    *len = 0;
    return grib_get_long(grib_handle_of_accessor(this), numberOfValues_, len);
}

int grib_accessor_data_g2shsimple_packing_t::unpack_double(double* val, size_t* len)
{
    int err = GRIB_SUCCESS;

    size_t n_vals = 0;

    if ((err = grib_get_size(grib_handle_of_accessor(this), coded_values_, &n_vals)) != GRIB_SUCCESS)
        return err;

    dirty_ = 0;

    /* n_vals = coded_n_vals+1; */

    if (*len < n_vals) {
        *len = n_vals;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if ((err = grib_get_double_internal(grib_handle_of_accessor(this), real_part_, val)) != GRIB_SUCCESS)
        return err;

    val++;

    if ((err = grib_get_double_array_internal(grib_handle_of_accessor(this), coded_values_, val, &n_vals)) != GRIB_SUCCESS)
        return err;

    *len = n_vals;

    return err;
}

int grib_accessor_data_g2shsimple_packing_t::pack_double(const double* val, size_t* len)
{
    int err = GRIB_SUCCESS;

    size_t coded_n_vals = *len - 1;
    size_t n_vals       = *len;

    if (*len == 0)
        return GRIB_NO_VALUES;

    dirty_ = 1;

    if ((err = grib_set_double_internal(grib_handle_of_accessor(this), real_part_, *val)) != GRIB_SUCCESS)
        return err;

    val++;

    if ((err = grib_set_double_array_internal(grib_handle_of_accessor(this), coded_values_, val, coded_n_vals)) != GRIB_SUCCESS)
        return err;

    *len = n_vals;

    if ((err = grib_set_long_internal(grib_handle_of_accessor(this), numberOfValues_, (long)n_vals)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(grib_handle_of_accessor(this), numberOfDataPoints_, (long)n_vals)) != GRIB_SUCCESS)
        return err;

    return err;
}
