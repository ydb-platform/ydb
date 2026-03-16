/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g1second_order_row_by_row_packing.h"

grib_accessor_data_g1second_order_row_by_row_packing_t _grib_accessor_data_g1second_order_row_by_row_packing{};
grib_accessor* grib_accessor_data_g1second_order_row_by_row_packing = &_grib_accessor_data_g1second_order_row_by_row_packing;

void grib_accessor_data_g1second_order_row_by_row_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);
    grib_handle* gh = grib_handle_of_accessor(this);

    half_byte_                       = args->get_name(gh, carg_++);
    packingType_                     = args->get_name(gh, carg_++);
    ieee_packing_                    = args->get_name(gh, carg_++);
    precision_                       = args->get_name(gh, carg_++);
    widthOfFirstOrderValues_         = args->get_name(gh, carg_++);
    N1_                              = args->get_name(gh, carg_++);
    N2_                              = args->get_name(gh, carg_++);
    numberOfGroups_                  = args->get_name(gh, carg_++);
    numberOfSecondOrderPackedValues_ = args->get_name(gh, carg_++);
    extraValues_                     = args->get_name(gh, carg_++);
    Ni_                              = args->get_name(gh, carg_++);
    Nj_                              = args->get_name(gh, carg_++);
    pl_                              = args->get_name(gh, carg_++);
    jPointsAreConsecutive_           = args->get_name(gh, carg_++);
    groupWidths_                     = args->get_name(gh, carg_++);
    bitmap_                          = args->get_name(gh, carg_++);
    edition_                         = 1;
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_g1second_order_row_by_row_packing_t::value_count(long* count)
{
    grib_handle* gh = grib_handle_of_accessor(this);
    long n = 0, i = 0;
    long numberOfRows          = 0;
    long jPointsAreConsecutive = 0;
    long Ni = 0, Nj = 0;
    int bitmapPresent = 0;
    size_t plSize     = 0;
    long* pl          = 0;
    int ret           = 0;
    grib_context* c   = context_;

    if (bitmap_)
        bitmapPresent = 1;
    if ((ret = grib_get_long_internal(gh, jPointsAreConsecutive_, &jPointsAreConsecutive)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, Ni_, &Ni)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, Nj_, &Nj)) != GRIB_SUCCESS)
        return ret;
    if (jPointsAreConsecutive) {
        numberOfRows = Ni;
    }
    else {
        numberOfRows = Nj;
    }

    plSize = 0;
    ret    = grib_get_size(gh, pl_, &plSize);
    if (ret == GRIB_SUCCESS) {
        pl = (long*)grib_context_malloc_clear(context_, sizeof(long) * plSize);
        if ((ret = grib_get_long_array(gh, pl_, pl, &plSize)) != GRIB_SUCCESS)
            return ret;
    }
    ret = 0;

    n = 0;
    if (bitmapPresent) {
        long *bitmap, *pbitmap;
        size_t numberOfPoints = 0;

        if (plSize && pl) {
            for (i = 0; i < numberOfRows; i++)
                numberOfPoints += pl[i];
            grib_context_free(c, pl);
        }
        else {
            numberOfPoints = Ni * Nj;
        }
        bitmap  = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfPoints);
        pbitmap = bitmap;
        grib_get_long_array(gh, bitmap_, bitmap, &numberOfPoints);
        for (i = 0; i < numberOfPoints; i++)
            n += *(bitmap++);

        grib_context_free(context_, pbitmap);
    }
    else {
        if (plSize) {
            if (numberOfRows && !pl) return GRIB_INTERNAL_ERROR;
            for (i = 0; i < numberOfRows; i++)
                n += pl[i];
            grib_context_free(c, pl);
        }
        else {
            n = Ni * Nj;
        }
    }

    *count = n;
    return ret;
}

template <typename T>
int grib_accessor_data_g1second_order_row_by_row_packing_t::unpack_real(T* values, size_t* len)
{
    grib_handle* gh                                              = grib_handle_of_accessor(this);
    int ret                                                      = 0;
    long numberOfGroups, numberOfSecondOrderPackedValues;
    long* groupWidths      = 0;
    long* firstOrderValues = 0;
    long* X                = 0;
    long numberOfRows, numberOfColumns;
    long* numbersPerRow;
    long pos                     = 0;
    long widthOfFirstOrderValues = 0;
    long jPointsAreConsecutive;
    unsigned char* buf = (unsigned char*)gh->buffer->data;
    long k, i, j, n, Ni, Nj;
    double reference_value;
    long binary_scale_factor;
    long decimal_scale_factor;
    double s, d;
    size_t groupWidthsSize = 0;
    int bitmapPresent      = 0;
    size_t plSize          = 0;
    long* pl               = 0;

    buf += byte_offset();
    if ((ret = grib_get_long_internal(gh, numberOfGroups_, &numberOfGroups)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, jPointsAreConsecutive_, &jPointsAreConsecutive)) != GRIB_SUCCESS)
        return ret;

    if (bitmap_)
        bitmapPresent = 1;
    ret = grib_get_size(gh, pl_, &plSize);
    if (ret == GRIB_SUCCESS) {
        pl = (long*)grib_context_malloc_clear(context_, sizeof(long) * plSize);
        if ((ret = grib_get_long_array(gh, pl_, pl, &plSize)) != GRIB_SUCCESS)
            return ret;
    }

    if ((ret = grib_get_long_internal(gh, Ni_, &Ni)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, Nj_, &Nj)) != GRIB_SUCCESS)
        return ret;
    if (jPointsAreConsecutive) {
        numberOfRows    = Ni;
        numberOfColumns = Nj;
    }
    else {
        numberOfRows    = Nj;
        numberOfColumns = Ni;
    }

    numbersPerRow = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfRows);
    if (!numbersPerRow)
        return GRIB_OUT_OF_MEMORY;
    if (bitmapPresent) {
        long *bitmap, *pbitmap;
        size_t numberOfPoints = Ni * Nj;

        if (plSize && pl) {
            numberOfPoints = 0;
            for (i = 0; i < numberOfRows; i++)
                numberOfPoints += pl[i];
        }
        bitmap  = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfPoints);
        pbitmap = bitmap;
        grib_get_long_array(gh, bitmap_, bitmap, &numberOfPoints);
        if (plSize && pl) {
            for (i = 0; i < numberOfRows; i++) {
                for (j = 0; j < pl[i]; j++) {
                    numbersPerRow[i] += *(bitmap++);
                }
            }
        }
        else {
            for (i = 0; i < numberOfRows; i++) {
                numbersPerRow[i] = 0;
                for (j = 0; j < Ni; j++) {
                    numbersPerRow[i] += *(bitmap++);
                }
            }
        }

        grib_context_free(context_, pbitmap);
    }
    else {
        if (plSize && pl) {
            for (i = 0; i < numberOfRows; i++)
                numbersPerRow[i] = pl[i];
        }
        else {
            for (i = 0; i < numberOfRows; i++)
                numbersPerRow[i] = numberOfColumns;
        }
    }

    if ((ret = grib_get_long_internal(gh, widthOfFirstOrderValues_, &widthOfFirstOrderValues)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(gh, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, numberOfSecondOrderPackedValues_,
                                      &numberOfSecondOrderPackedValues)) != GRIB_SUCCESS)
        return ret;

    groupWidths     = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    groupWidthsSize = numberOfGroups;
    if ((ret = grib_get_long_array_internal(gh, groupWidths_, groupWidths, &groupWidthsSize)) != GRIB_SUCCESS)
        return ret;

    firstOrderValues = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    grib_decode_long_array(buf, &pos, widthOfFirstOrderValues, numberOfGroups, firstOrderValues);
    pos = 8 * ((pos + 7) / 8);

    n = 0;
    for (i = 0; i < numberOfGroups; i++)
        n += numbersPerRow[i];

    if (*len < (size_t)n)
        return GRIB_ARRAY_TOO_SMALL;

    X = (long*)grib_context_malloc_clear(context_, sizeof(long) * n);
    n = 0;
    k = 0;
    for (i = 0; i < numberOfGroups; i++) {
        if (groupWidths[i] > 0) {
            for (j = 0; j < numbersPerRow[k]; j++) {
                X[n] = grib_decode_unsigned_long(buf, &pos, groupWidths[i]);
                X[n] += firstOrderValues[i];
                n++;
            }
        }
        else {
            for (j = 0; j < numbersPerRow[k]; j++) {
                X[n] = firstOrderValues[i];
                n++;
            }
        }
        k++;
    }

    s = codes_power<T>(binary_scale_factor, 2);
    d = codes_power<T>(-decimal_scale_factor, 10);
    for (i = 0; i < n; i++) {
        values[i] = (T)(((X[i] * s) + reference_value) * d);
    }
    grib_context_free(context_, firstOrderValues);
    grib_context_free(context_, X);
    grib_context_free(context_, groupWidths);
    if (plSize)
        grib_context_free(context_, pl);
    if (numbersPerRow)
        grib_context_free(context_, numbersPerRow);

    return ret;
}

int grib_accessor_data_g1second_order_row_by_row_packing_t::unpack_float(float* values, size_t* len)
{
    return unpack_real<float>(values, len);
}

int grib_accessor_data_g1second_order_row_by_row_packing_t::unpack_double(double* values, size_t* len)
{
    return unpack_real<double>(values, len);
}

int grib_accessor_data_g1second_order_row_by_row_packing_t::pack_double(const double* cval, size_t* len)
{
    int err         = 0;
    grib_handle* gh = grib_handle_of_accessor(this);
    char type[]     = "grid_second_order";
    size_t size     = strlen(type);

    err = grib_set_string(gh, "packingType", type, &size);
    if (err)
        return err;

    return grib_set_double_array(gh, "values", cval, *len);
}
