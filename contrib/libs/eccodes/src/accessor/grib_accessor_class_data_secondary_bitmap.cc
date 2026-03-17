/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_secondary_bitmap.h"

grib_accessor_data_secondary_bitmap_t _grib_accessor_data_secondary_bitmap{};
grib_accessor* grib_accessor_data_secondary_bitmap = &_grib_accessor_data_secondary_bitmap;

void grib_accessor_data_secondary_bitmap_t::init(const long v, grib_arguments* args)
{
    grib_accessor_gen_t::init(v, args);
    primary_bitmap_   = args->get_name(grib_handle_of_accessor(this), 0);
    secondary_bitmap_ = args->get_name(grib_handle_of_accessor(this), 1);
    missing_value_    = args->get_name(grib_handle_of_accessor(this), 2);
    expand_by_        = args->get_name(grib_handle_of_accessor(this), 3);

    length_ = 0;
}

void grib_accessor_data_secondary_bitmap_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_values(this);
}

int grib_accessor_data_secondary_bitmap_t::unpack_double(double* val, size_t* len)
{
    size_t i       = 0;
    size_t j       = 0;
    size_t k       = 0;
    size_t m       = 0;
    size_t n_vals  = 0;
    long nn        = 0;
    long expand_by = 0;
    int err        = 0;
    size_t primary_len;
    size_t secondary_len;
    double* primary_vals;
    double* secondary_vals;
    err    = value_count(&nn);
    n_vals = nn;
    if (err)
        return err;

    if (*len < n_vals) {
        *len = n_vals;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if ((err = grib_get_long(grib_handle_of_accessor(this), expand_by_, &expand_by)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_size(grib_handle_of_accessor(this), primary_bitmap_, &primary_len)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_size(grib_handle_of_accessor(this), secondary_bitmap_, &secondary_len)) != GRIB_SUCCESS)
        return err;

    primary_vals = (double*)grib_context_malloc(context_, primary_len * sizeof(double));
    if (!primary_vals)
        return GRIB_OUT_OF_MEMORY;

    secondary_vals = (double*)grib_context_malloc(context_, secondary_len * sizeof(double));
    if (!secondary_vals) {
        grib_context_free(context_, primary_vals);
        return GRIB_OUT_OF_MEMORY;
    }

    if ((err = grib_get_double_array_internal(grib_handle_of_accessor(this), primary_bitmap_, primary_vals, &primary_len)) != GRIB_SUCCESS) {
        grib_context_free(context_, secondary_vals);
        grib_context_free(context_, primary_vals);
        return err;
    }

    if ((err = grib_get_double_array_internal(grib_handle_of_accessor(this), secondary_bitmap_, secondary_vals, &secondary_len)) != GRIB_SUCCESS) {
        grib_context_free(context_, secondary_vals);
        grib_context_free(context_, primary_vals);
        return err;
    }

    k = 0;
    m = 0;
    for (i = 0; i < primary_len; i++) {
        /* if(primary_vals[i]) f++; */
        if (primary_vals[i]) {
            for (j = 0; j < expand_by; j++)
                val[k++] = secondary_vals[m++];
        }
        else {
            for (j = 0; j < expand_by; j++)
                val[k++] = 0;
        }
    }

    ECCODES_ASSERT(k <= *len);
    ECCODES_ASSERT(m <= secondary_len);

    /*printf("FOOBAR %d %d %ld %d\n",f,primary_len,expand_by,n_vals);*/

    *len = n_vals;

    grib_context_free(context_, primary_vals);
    grib_context_free(context_, secondary_vals);
    return err;
}

long grib_accessor_data_secondary_bitmap_t::get_native_type()
{
    // grib_accessor_data_secondary_bitmap_t* self =  (grib_accessor_data_secondary_bitmap_t*)a;
    // return grib_accessor_get_native_type(grib_find_accessor(grib_handle_of_accessor(this),coded_values_ ));
    return GRIB_TYPE_DOUBLE;
}
