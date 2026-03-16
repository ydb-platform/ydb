/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_apply_boustrophedonic.h"

grib_accessor_data_apply_boustrophedonic_t _grib_accessor_data_apply_boustrophedonic{};
grib_accessor* grib_accessor_data_apply_boustrophedonic = &_grib_accessor_data_apply_boustrophedonic;

void grib_accessor_data_apply_boustrophedonic_t::init(const long v, grib_arguments* args)
{
    grib_accessor_gen_t::init(v, args);

    int n            = 0;
    values_          = args->get_name(grib_handle_of_accessor(this), n++);
    numberOfRows_    = args->get_name(grib_handle_of_accessor(this), n++);
    numberOfColumns_ = args->get_name(grib_handle_of_accessor(this), n++);
    numberOfPoints_  = args->get_name(grib_handle_of_accessor(this), n++);
    pl_              = args->get_name(grib_handle_of_accessor(this), n++);

    length_ = 0;
}
void grib_accessor_data_apply_boustrophedonic_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_values(this);
}

int grib_accessor_data_apply_boustrophedonic_t::value_count(long* numberOfPoints)
{
    *numberOfPoints = 0;
    return grib_get_long_internal(grib_handle_of_accessor(this), numberOfPoints_, numberOfPoints);
}

template <typename T>
int grib_accessor_data_apply_boustrophedonic_t::unpack(T* val, size_t* len)
{
    size_t plSize                                    = 0;
    long* pl                                         = 0;
    double* values                                   = 0;
    double* pvalues                                  = 0;
    T* pval                                          = 0;
    size_t valuesSize                                = 0;
    long i, j;
    int ret;
    long numberOfPoints, numberOfRows, numberOfColumns;

    ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfPoints_, &numberOfPoints);
    if (ret)
        return ret;

    if (*len < numberOfPoints) {
        *len = numberOfPoints;
        return GRIB_ARRAY_TOO_SMALL;
    }

    ret = grib_get_size(grib_handle_of_accessor(this), values_, &valuesSize);
    if (ret)
        return ret;

    /* constant field */
    if (valuesSize == 0)
        return 0;

    if (valuesSize != numberOfPoints) {
        grib_context_log(context_, GRIB_LOG_ERROR, "boustrophedonic ordering error: ( %s=%ld ) != (sizeOf(%s)=%ld)",
                         numberOfPoints, numberOfPoints, values_, (long)valuesSize);
        return GRIB_DECODING_ERROR;
    }

    values = (double*)grib_context_malloc_clear(context_, sizeof(double) * numberOfPoints);
    ret    = grib_get_double_array_internal(grib_handle_of_accessor(this), values_, values, &valuesSize);
    if (ret)
        return ret;

    pvalues = values;
    pval    = val;

    ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfRows_, &numberOfRows);
    if (ret)
        return ret;

    ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfColumns_, &numberOfColumns);
    if (ret)
        return ret;

    if (grib_get_size(grib_handle_of_accessor(this), pl_, &plSize) == GRIB_SUCCESS) {
        ECCODES_ASSERT(plSize == numberOfRows);
        pl  = (long*)grib_context_malloc_clear(context_, sizeof(long) * plSize);
        ret = grib_get_long_array_internal(grib_handle_of_accessor(this), pl_, pl, &plSize);
        if (ret)
            return ret;

        for (j = 0; j < numberOfRows; j++) {
            if (j % 2) {
                pval += pl[j];
                for (i = 0; i < pl[j]; i++)
                    *(pval--) = *(pvalues++);
                pval += pl[j];
            }
            else {
                for (i = 0; i < pl[j]; i++)
                    *(pval++) = *(pvalues++);
            }
        }

        grib_context_free(context_, pl);
    }
    else {
        for (j = 0; j < numberOfRows; j++) {
            if (j % 2) {
                pval += numberOfColumns - 1;
                for (i = 0; i < numberOfColumns; i++)
                    *(pval--) = *(pvalues++);
                pval += numberOfColumns + 1;
            }
            else {
                for (i = 0; i < numberOfColumns; i++)
                    *(pval++) = *(pvalues++);
            }
        }
    }

    grib_context_free(context_, values);

    return GRIB_SUCCESS;
}

int grib_accessor_data_apply_boustrophedonic_t::unpack_double(double* val, size_t* len)
{
    return unpack<double>(val, len);
}

int grib_accessor_data_apply_boustrophedonic_t::unpack_float(float* val, size_t* len)
{
    return unpack<float>(val, len);
}

int grib_accessor_data_apply_boustrophedonic_t::unpack_double_element(size_t idx, double* val)
{
    size_t size;
    double* values;

    /* GRIB-564: The index idx relates to codedValues NOT values! */
    int err = grib_get_size(parent_->h, "codedValues", &size);
    if (err)
        return err;
    if (idx > size)
        return GRIB_INVALID_NEAREST;

    values = (double*)grib_context_malloc_clear(parent_->h->context, size * sizeof(double));
    err    = grib_get_double_array(parent_->h, "codedValues", values, &size);
    if (err) {
        grib_context_free(parent_->h->context, values);
        return err;
    }
    *val = values[idx];
    grib_context_free(parent_->h->context, values);
    return GRIB_SUCCESS;
}

int grib_accessor_data_apply_boustrophedonic_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    size_t size = 0, i = 0;
    double* values;
    int err = 0;

    /* GRIB-564: The indexes in index_array relate to codedValues NOT values! */
    err = grib_get_size(grib_handle_of_accessor(this), "codedValues", &size);
    if (err)
        return err;

    for (i = 0; i < len; i++) {
        if (index_array[i] > size) return GRIB_INVALID_ARGUMENT;
    }

    values = (double*)grib_context_malloc_clear(context_, size * sizeof(double));
    err    = grib_get_double_array(grib_handle_of_accessor(this), "codedValues", values, &size);
    if (err) {
        grib_context_free(context_, values);
        return err;
    }
    for (i = 0; i < len; i++) {
        val_array[i] = values[index_array[i]];
    }
    grib_context_free(context_, values);
    return GRIB_SUCCESS;
}

int grib_accessor_data_apply_boustrophedonic_t::pack_double(const double* val, size_t* len)
{
    size_t plSize     = 0;
    long* pl          = 0;
    double* values    = 0;
    double* pvalues   = 0;
    double* pval      = 0;
    size_t valuesSize = 0;
    long i, j;
    long numberOfPoints, numberOfRows, numberOfColumns;

    int ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfPoints_, &numberOfPoints);
    if (ret)
        return ret;

    if (*len < numberOfPoints) {
        *len = numberOfPoints;
        return GRIB_ARRAY_TOO_SMALL;
    }

    valuesSize = numberOfPoints;

    values = (double*)grib_context_malloc_clear(context_, sizeof(double) * numberOfPoints);

    pvalues = values;
    pval    = (double*)val;

    ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfRows_, &numberOfRows);
    if (ret)
        return ret;

    ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfColumns_, &numberOfColumns);
    if (ret)
        return ret;

    if (grib_get_size(grib_handle_of_accessor(this), pl_, &plSize) == GRIB_SUCCESS) {
        ECCODES_ASSERT(plSize == numberOfRows);
        pl  = (long*)grib_context_malloc_clear(context_, sizeof(long) * plSize);
        ret = grib_get_long_array_internal(grib_handle_of_accessor(this), pl_, pl, &plSize);
        if (ret)
            return ret;

        for (j = 0; j < numberOfRows; j++) {
            if (j % 2) {
                pvalues += pl[j];
                for (i = 0; i < pl[j]; i++) {
                    *(--pvalues) = *(pval++);
                }
                pvalues += pl[j];
            }
            else {
                for (i = 0; i < pl[j]; i++)
                    *(pvalues++) = *(pval++);
            }
        }

        grib_context_free(context_, pl);
    }
    else {
        for (j = 0; j < numberOfRows; j++) {
            if (j % 2) {
                pvalues += numberOfColumns;
                for (i = 0; i < numberOfColumns; i++)
                    *(--pvalues) = *(pval++);
                pvalues += numberOfColumns;
            }
            else {
                for (i = 0; i < numberOfColumns; i++)
                    *(pvalues++) = *(pval++);
            }
        }
    }
    ret = grib_set_double_array_internal(grib_handle_of_accessor(this), values_, values, valuesSize);
    if (ret)
        return ret;

    grib_context_free(context_, values);

    return GRIB_SUCCESS;
}

long grib_accessor_data_apply_boustrophedonic_t::get_native_type()
{
    return GRIB_TYPE_DOUBLE;
}
