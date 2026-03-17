/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_shsimple_packing.h"

grib_accessor_data_shsimple_packing_t _grib_accessor_data_shsimple_packing{};
grib_accessor* grib_accessor_data_shsimple_packing = &_grib_accessor_data_shsimple_packing;

void grib_accessor_data_shsimple_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_gen_t::init(v, args);

    coded_values_ = args->get_name(grib_handle_of_accessor(this), 0);
    real_part_    = args->get_name(grib_handle_of_accessor(this), 1);
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;

    length_ = 0;
}

void grib_accessor_data_shsimple_packing_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_values(this);
}

int grib_accessor_data_shsimple_packing_t::pack_double(const double* val, size_t* len)
{
    int err = GRIB_SUCCESS;

    size_t coded_n_vals = *len - 1;
    size_t n_vals       = *len;

    dirty_ = 1;

    if (*len == 0)
        return GRIB_NO_VALUES;

    if ((err = grib_set_double_internal(grib_handle_of_accessor(this), real_part_, *val)) != GRIB_SUCCESS)
        return err;

    val++;

    if ((err = grib_set_double_array_internal(grib_handle_of_accessor(this), coded_values_, val, coded_n_vals)) != GRIB_SUCCESS)
        return err;

    *len = n_vals;

    return err;
}

long grib_accessor_data_shsimple_packing_t::get_native_type()
{
    return GRIB_TYPE_DOUBLE;
}
