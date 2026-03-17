/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g2secondary_bitmap.h"

grib_accessor_data_g2secondary_bitmap_t _grib_accessor_data_g2secondary_bitmap{};
grib_accessor* grib_accessor_data_g2secondary_bitmap = &_grib_accessor_data_g2secondary_bitmap;

void grib_accessor_data_g2secondary_bitmap_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_secondary_bitmap_t::init(v, args);
    number_of_values_ = args->get_name(grib_handle_of_accessor(this), 4);
}

int grib_accessor_data_g2secondary_bitmap_t::value_count(long* len)
{
    *len = 0;
    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, len);
}

int grib_accessor_data_g2secondary_bitmap_t::pack_double(const double* val, size_t* len)
{
    int err = 0;

    long primary_len = 0, secondary_len = 0;
    double* primary_bitmap   = NULL;
    double* secondary_bitmap = NULL;
    long i = 0, j = 0, k = 0, m = 0;
    double missing_value = 0, present_value = 0;
    long expand_by = 0;

    if (*len == 0)
        return GRIB_NO_VALUES;

    if ((err = grib_get_long(grib_handle_of_accessor(this), expand_by_, &expand_by)) != GRIB_SUCCESS)
        return err;
    if (expand_by <= 0)
        return GRIB_ENCODING_ERROR;

    if ((err = grib_get_double_internal(grib_handle_of_accessor(this), missing_value_, &missing_value)) != GRIB_SUCCESS)
        return err;

    if (*len % expand_by) {
        /*TODO: issue warning */
        return GRIB_ENCODING_ERROR;
    }

    primary_len    = *len / expand_by;
    primary_bitmap = (double*)grib_context_malloc_clear(context_, primary_len * sizeof(double));
    if (!primary_bitmap)
        return GRIB_OUT_OF_MEMORY;

    secondary_len    = *len;
    secondary_bitmap = (double*)grib_context_malloc_clear(context_, secondary_len * sizeof(double));
    if (!secondary_bitmap) {
        grib_context_free(context_, primary_bitmap);
        return GRIB_OUT_OF_MEMORY;
    }

    if (missing_value == 0)
        present_value = 1;
    else
        present_value = 0;

    k = 0;
    m = 0;
    for (i = 0; i < *len; i += expand_by) {
        int cnt = 0;
        for (j = 0; j < expand_by; j++)
            if (val[i + j] == missing_value)
                cnt++;

        if (cnt == expand_by) /* all expand_by values are missing */
            primary_bitmap[k++] = missing_value;
        else {
            primary_bitmap[k++] = present_value;
            for (j = 0; j < expand_by; j++)
                secondary_bitmap[m++] = val[i + j];
            // on++;
        }
    }

    *len = k;

    ECCODES_ASSERT(k == primary_len);

    err = grib_set_double_array_internal(grib_handle_of_accessor(this), primary_bitmap_, primary_bitmap, k);
    if (err == GRIB_SUCCESS)
        err = grib_set_double_array_internal(grib_handle_of_accessor(this), secondary_bitmap_, secondary_bitmap, m);

    grib_context_free(context_, primary_bitmap);
    grib_context_free(context_, secondary_bitmap);

    if (err == GRIB_SUCCESS)
        err = grib_set_long_internal(grib_handle_of_accessor(this), number_of_values_, *len * expand_by);

    return err;
}
