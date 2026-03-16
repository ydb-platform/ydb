/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_apply_boustrophedonic_bitmap.h"

grib_accessor_data_apply_boustrophedonic_bitmap_t _grib_accessor_data_apply_boustrophedonic_bitmap{};
grib_accessor* grib_accessor_data_apply_boustrophedonic_bitmap = &_grib_accessor_data_apply_boustrophedonic_bitmap;

void grib_accessor_data_apply_boustrophedonic_bitmap_t::init(const long v, grib_arguments* args)
{
    grib_accessor_gen_t::init(v, args);
    int n           = 0;
    grib_handle* gh = grib_handle_of_accessor(this);

    coded_values_        = args->get_name(gh, n++);
    bitmap_              = args->get_name(gh, n++);
    missing_value_       = args->get_name(gh, n++);
    binary_scale_factor_ = args->get_name(gh, n++);

    numberOfRows_    = args->get_name(gh, n++);
    numberOfColumns_ = args->get_name(gh, n++);
    numberOfPoints_  = args->get_name(gh, n++);

    length_ = 0;
}

void grib_accessor_data_apply_boustrophedonic_bitmap_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_values(this);
}

int grib_accessor_data_apply_boustrophedonic_bitmap_t::value_count(long* count)
{
    grib_handle* gh = grib_handle_of_accessor(this);
    size_t len      = 0;
    int ret         = 0;

    /* This accessor is for data with a bitmap after all */
    ECCODES_ASSERT(grib_find_accessor(gh, bitmap_));

    ret    = grib_get_size(gh, bitmap_, &len);
    *count = len;
    return ret;
}

int grib_accessor_data_apply_boustrophedonic_bitmap_t::unpack_double(double* val, size_t* len)
{
    grib_handle* gh = grib_handle_of_accessor(this);

    size_t i = 0, j = 0, n_vals = 0, irow = 0;
    long nn              = 0;
    int err              = 0;
    size_t coded_n_vals  = 0;
    double* coded_vals   = NULL;
    double missing_value = 0;
    long numberOfPoints, numberOfRows, numberOfColumns;

    err    = value_count(&nn);
    n_vals = nn;
    if (err)
        return err;

    err = grib_get_long_internal(gh, numberOfRows_, &numberOfRows);
    if (err)
        return err;
    err = grib_get_long_internal(gh, numberOfColumns_, &numberOfColumns);
    if (err)
        return err;
    err = grib_get_long_internal(gh, numberOfPoints_, &numberOfPoints);
    if (err)
        return err;
    ECCODES_ASSERT(nn == numberOfPoints);

    if (!grib_find_accessor(gh, bitmap_))
        return grib_get_double_array_internal(gh, coded_values_, val, len);

    if ((err = grib_get_size(gh, coded_values_, &coded_n_vals)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_double_internal(gh, missing_value_, &missing_value)) != GRIB_SUCCESS)
        return err;

    if (*len < n_vals) {
        *len = n_vals;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if (coded_n_vals == 0) {
        for (i = 0; i < n_vals; i++)
            val[i] = missing_value;

        *len = n_vals;
        return GRIB_SUCCESS;
    }

    if ((err = grib_get_double_array_internal(gh, bitmap_, val, &n_vals)) != GRIB_SUCCESS)
        return err;

    coded_vals = (double*)grib_context_malloc(context_, coded_n_vals * sizeof(double));
    if (coded_vals == NULL)
        return GRIB_OUT_OF_MEMORY;

    if ((err = grib_get_double_array_internal(gh, coded_values_, coded_vals, &coded_n_vals)) != GRIB_SUCCESS) {
        grib_context_free(context_, coded_vals);
        return err;
    }

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "grib_accessor_data_apply_boustrophedonic_bitmap: unpack_double : creating %s, %d values",
                     name_, n_vals);

    /* Boustrophedonic ordering (See GRIB-472):
     * Values on even rank lines (the initial line scanned having rank 1) are swapped
     */
    for (irow = 0; irow < numberOfRows; ++irow) {
        if (irow % 2) {
            /* Reverse bitmap entries */
            size_t k     = 0;
            size_t start = irow * numberOfColumns;
            size_t end   = start + numberOfColumns - 1;
            size_t mid   = (numberOfColumns - 1) / 2;
            for (k = 0; k < mid; ++k) {
                /* Swap value at either end */
                double temp    = val[start + k];
                val[start + k] = val[end - k];
                val[end - k]   = temp;
            }
        }
    }

    for (i = 0; i < n_vals; i++) {
        if (val[i] == 0) {
            val[i] = missing_value;
        }
        else {
            val[i] = coded_vals[j++];
            if (j > coded_n_vals) {
                grib_context_free(context_, coded_vals);
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "grib_accessor_data_apply_boustrophedonic_bitmap [%s]:"
                                 " unpack_double :  number of coded values does not match bitmap %ld %ld",
                                 name_, coded_n_vals, n_vals);

                return GRIB_ARRAY_TOO_SMALL;
            }
        }
    }

    *len = n_vals;

    grib_context_free(context_, coded_vals);
    return err;
}

int grib_accessor_data_apply_boustrophedonic_bitmap_t::unpack_double_element(size_t idx, double* val)
{
    grib_handle* gh = grib_handle_of_accessor(this);
    int err = 0, i = 0;
    size_t cidx          = 0;
    double missing_value = 0;
    double* bvals        = NULL;
    size_t n_vals        = 0;
    long nn              = 0;

    err    = value_count(&nn);
    n_vals = nn;
    if (err)
        return err;

    if (!grib_find_accessor(gh, bitmap_))
        return grib_get_double_element_internal(gh, coded_values_, idx, val);

    if ((err = grib_get_double_internal(gh, missing_value_, &missing_value)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_double_element_internal(gh, bitmap_, idx, val)) != GRIB_SUCCESS)
        return err;

    if (*val == 0) {
        *val = missing_value;
        return GRIB_SUCCESS;
    }

    bvals = (double*)grib_context_malloc(context_, n_vals * sizeof(double));
    if (bvals == NULL)
        return GRIB_OUT_OF_MEMORY;

    if ((err = grib_get_double_array_internal(gh, bitmap_, bvals, &n_vals)) != GRIB_SUCCESS)
        return err;

    cidx = 0;
    for (i = 0; i < idx; i++) {
        cidx += bvals[i];
    }

    grib_context_free(context_, bvals);

    return grib_get_double_element_internal(gh, coded_values_, cidx, val);
}

int grib_accessor_data_apply_boustrophedonic_bitmap_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    grib_handle* gh = grib_handle_of_accessor(this);
    int err = 0, all_missing = 1;
    size_t cidx          = 0;    /* index into the coded_values array */
    size_t* cidx_array   = NULL; /* array of indexes into the coded_values */
    double* cval_array   = NULL; /* array of values of the coded_values */
    double missing_value = 0;
    double* bvals        = NULL;
    size_t n_vals = 0, i = 0, j = 0, idx = 0, count_1s = 0, ci = 0;
    long nn = 0;

    err    = value_count(&nn);
    n_vals = nn;
    if (err) return err;

    if (!grib_find_accessor(gh, bitmap_))
        return grib_get_double_element_set_internal(gh, coded_values_, index_array, len, val_array);

    if ((err = grib_get_double_internal(gh, missing_value_, &missing_value)) != GRIB_SUCCESS)
        return err;

    err = grib_get_double_element_set_internal(gh, bitmap_, index_array, len, val_array);
    if (err) return err;
    for (i = 0; i < len; i++) {
        if (val_array[i] == 0) {
            val_array[i] = missing_value;
        }
        else {
            all_missing = 0;
            count_1s++;
        }
    }

    if (all_missing) {
        return GRIB_SUCCESS;
    }

    /* At this point val_array contains entries which are either missing_value or 1 */
    /* Now we need to dig into the codes values with index array of count_1s */

    bvals = (double*)grib_context_malloc(context_, n_vals * sizeof(double));
    if (!bvals) return GRIB_OUT_OF_MEMORY;

    if ((err = grib_get_double_array_internal(gh, bitmap_, bvals, &n_vals)) != GRIB_SUCCESS)
        return err;

    cidx_array = (size_t*)grib_context_malloc(context_, count_1s * sizeof(size_t));
    cval_array = (double*)grib_context_malloc(context_, count_1s * sizeof(double));

    ci = 0;
    for (i = 0; i < len; i++) {
        if (val_array[i] == 1) {
            idx  = index_array[i];
            cidx = 0;
            for (j = 0; j < idx; j++) {
                cidx += bvals[j];
            }
            ECCODES_ASSERT(ci < count_1s);
            cidx_array[ci++] = cidx;
        }
    }
    err = grib_get_double_element_set_internal(gh, coded_values_, cidx_array, count_1s, cval_array);
    if (err) return err;

    /* Transfer from cval_array to our result val_array */
    ci = 0;
    for (i = 0; i < len; i++) {
        if (val_array[i] == 1) {
            val_array[i] = cval_array[ci++];
        }
    }

    grib_context_free(context_, bvals);
    grib_context_free(context_, cidx_array);
    grib_context_free(context_, cval_array);

    return GRIB_SUCCESS;
}

int grib_accessor_data_apply_boustrophedonic_bitmap_t::pack_double(const double* val, size_t* len)
{
    grib_handle* gh    = grib_handle_of_accessor(this);
    int err            = 0;
    size_t bmaplen     = *len;
    size_t irow        = 0;
    long coded_n_vals  = 0;
    double* coded_vals = NULL;
    double* values     = 0;
    long i             = 0;
    long j             = 0;
    long numberOfPoints, numberOfRows, numberOfColumns;
    double missing_value = 0;

    if (*len == 0)
        return GRIB_NO_VALUES;

    if (!grib_find_accessor(gh, bitmap_)) {
        err = grib_set_double_array_internal(gh, coded_values_, val, *len);
        /*printf("SETTING TOTAL number_of_data_points %s %ld\n",number_of_data_points_ ,*len);*/
        /*if(number_of_data_points_ )
            grib_set_long_internal(gh,number_of_data_points_ ,*len);*/
        return err;
    }

    if ((err = grib_get_double_internal(gh, missing_value_, &missing_value)) != GRIB_SUCCESS)
        return err;

    err = grib_get_long_internal(gh, numberOfRows_, &numberOfRows);
    if (err)
        return err;
    err = grib_get_long_internal(gh, numberOfColumns_, &numberOfColumns);
    if (err)
        return err;
    err = grib_get_long_internal(gh, numberOfPoints_, &numberOfPoints);
    if (err)
        return err;
    ECCODES_ASSERT(numberOfPoints == bmaplen);

    /* Create a copy of the incoming 'val' array because we're going to change it */
    values = (double*)grib_context_malloc_clear(context_, sizeof(double) * numberOfPoints);
    if (!values)
        return GRIB_OUT_OF_MEMORY;
    for (i = 0; i < numberOfPoints; ++i) {
        values[i] = val[i];
    }

    /* Boustrophedonic ordering must be applied to the bitmap (See GRIB-472) */
    for (irow = 0; irow < numberOfRows; ++irow) {
        if (irow % 2) {
            size_t k     = 0;
            size_t start = irow * numberOfColumns;
            size_t end   = start + numberOfColumns - 1;
            size_t mid   = (numberOfColumns - 1) / 2;
            for (k = 0; k < mid; ++k) {
                double temp       = values[start + k];
                values[start + k] = values[end - k];
                values[end - k]   = temp;
            }
        }
    }
    /* Now set the bitmap based on the array with the boustrophedonic ordering */
    if ((err = grib_set_double_array_internal(gh, bitmap_, values, bmaplen)) != GRIB_SUCCESS)
        return err;

    grib_context_free(context_, values);

    coded_n_vals = *len;

    if (coded_n_vals < 1) {
        err = grib_set_double_array_internal(gh, coded_values_, NULL, 0);
        return err;
    }

    coded_vals = (double*)grib_context_malloc_clear(context_, coded_n_vals * sizeof(double));
    if (!coded_vals)
        return GRIB_OUT_OF_MEMORY;

    for (i = 0; i < *len; i++) {
        /* To set the coded values, look at 'val' (the original array) */
        /* NOT 'values' (bitmap) which we swapped about */
        if (val[i] != missing_value) {
            coded_vals[j++] = val[i];
        }
    }

    err = grib_set_double_array_internal(gh, coded_values_, coded_vals, j);
    if (j == 0) {
        /*if (number_of_values_ )
            err=grib_set_long_internal(gh,number_of_values_ ,0);*/
        if (binary_scale_factor_)
            err = grib_set_long_internal(gh, binary_scale_factor_, 0);
    }

    grib_context_free(context_, coded_vals);

    return err;
}

long grib_accessor_data_apply_boustrophedonic_bitmap_t::get_native_type()
{
    // grib_accessor_data_apply_boustrophedonic_bitmap_t* self =  (grib_accessor_data_apply_boustrophedonic_bitmap_t*)a;
    // return grib_accessor_get_native_type(grib_find_accessor(grib_handle_of_accessor(this),coded_values_ ));

    return GRIB_TYPE_DOUBLE;
}
