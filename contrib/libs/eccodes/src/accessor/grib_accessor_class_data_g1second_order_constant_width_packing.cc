/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g1second_order_constant_width_packing.h"
#include "grib_scaling.h"

grib_accessor_data_g1second_order_constant_width_packing_t _grib_accessor_data_g1second_order_constant_width_packing{};
grib_accessor* grib_accessor_data_g1second_order_constant_width_packing = &_grib_accessor_data_g1second_order_constant_width_packing;

void grib_accessor_data_g1second_order_constant_width_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);
    grib_handle* hand = grib_handle_of_accessor(this);

    half_byte_                       = args->get_name(hand, carg_++);
    packingType_                     = args->get_name(hand, carg_++);
    ieee_packing_                    = args->get_name(hand, carg_++);
    precision_                       = args->get_name(hand, carg_++);
    widthOfFirstOrderValues_         = args->get_name(hand, carg_++);
    N1_                              = args->get_name(hand, carg_++);
    N2_                              = args->get_name(hand, carg_++);
    numberOfGroups_                  = args->get_name(hand, carg_++);
    numberOfSecondOrderPackedValues_ = args->get_name(hand, carg_++);
    extraValues_                     = args->get_name(hand, carg_++);
    Ni_                              = args->get_name(hand, carg_++);
    Nj_                              = args->get_name(hand, carg_++);
    pl_                              = args->get_name(hand, carg_++);
    jPointsAreConsecutive_           = args->get_name(hand, carg_++);
    bitmap_                          = args->get_name(hand, carg_++);
    groupWidth_                      = args->get_name(hand, carg_++);
    edition_                         = 1;
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_g1second_order_constant_width_packing_t::value_count(long* numberOfSecondOrderPackedValues)
{
    int err                          = 0;
    *numberOfSecondOrderPackedValues = 0;

    err = grib_get_long_internal(grib_handle_of_accessor(this), numberOfSecondOrderPackedValues_, numberOfSecondOrderPackedValues);

    return err;
}

int grib_accessor_data_g1second_order_constant_width_packing_t::unpack_float(float* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_data_g1second_order_constant_width_packing_t::unpack_double(double* values, size_t* len)
{
    int ret = 0;
    long numberOfGroups, numberOfSecondOrderPackedValues;
    long groupWidth              = 0;
    long* firstOrderValues       = 0;
    long* X                      = 0;
    long numberPerRow            = 0;
    long pos                     = 0;
    long widthOfFirstOrderValues = 0;
    long jPointsAreConsecutive;
    unsigned char* buf = (unsigned char*)grib_handle_of_accessor(this)->buffer->data;
    long i, n;
    double reference_value;
    long binary_scale_factor;
    long decimal_scale_factor;
    double s, d;
    long* secondaryBitmap;
    grib_handle* hand = grib_handle_of_accessor(this);

    buf += byte_offset();
    if ((ret = grib_get_long_internal(hand, numberOfGroups_, &numberOfGroups)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, jPointsAreConsecutive_, &jPointsAreConsecutive)) != GRIB_SUCCESS)
        return ret;

    if (jPointsAreConsecutive) {
        if ((ret = grib_get_long_internal(hand, Ni_, &numberPerRow)) != GRIB_SUCCESS)
            return ret;
    }
    else {
        if ((ret = grib_get_long_internal(hand, Nj_, &numberPerRow)) != GRIB_SUCCESS)
            return ret;
    }

    if ((ret = grib_get_long_internal(hand, widthOfFirstOrderValues_, &widthOfFirstOrderValues)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(hand, numberOfSecondOrderPackedValues_,
                                      &numberOfSecondOrderPackedValues)) != GRIB_SUCCESS)
        return ret;

    if (*len < numberOfSecondOrderPackedValues)
        return GRIB_ARRAY_TOO_SMALL;

    if ((ret = grib_get_long_internal(hand, groupWidth_, &groupWidth)) != GRIB_SUCCESS)
        return ret;

    secondaryBitmap = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfSecondOrderPackedValues);
    if (!secondaryBitmap)
        return GRIB_OUT_OF_MEMORY;

    grib_decode_long_array(buf, &pos, 1, numberOfSecondOrderPackedValues, secondaryBitmap);
    pos = 8 * ((pos + 7) / 8);

    firstOrderValues = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    if (!firstOrderValues)
        return GRIB_OUT_OF_MEMORY;

    grib_decode_long_array(buf, &pos, widthOfFirstOrderValues, numberOfGroups, firstOrderValues);
    pos = 8 * ((pos + 7) / 8);

    X = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfSecondOrderPackedValues);
    if (!X)
        return GRIB_OUT_OF_MEMORY;

    if (groupWidth > 0) {
        grib_decode_long_array(buf, &pos, groupWidth, numberOfSecondOrderPackedValues, X);
        n = 0;
        i = -1;
        while (n < numberOfSecondOrderPackedValues) {
            i += secondaryBitmap[n];
            long fovi = 0;
            // ECC-1703
            if (i >= 0 && i < numberOfGroups)
                fovi = firstOrderValues[i];
            X[n] = fovi + X[n];
            n++;
        }
    }
    else {
        n = 0;
        i = -1;
        while (n < numberOfSecondOrderPackedValues) {
            i += secondaryBitmap[n];
            long fovi = 0;
            if (i >= 0 && i < numberOfGroups)
                fovi = firstOrderValues[i];
            X[n] = fovi;
            n++;
        }
    }

    /*{
        long extrabits = 16 * ( (pos + 15 ) / 16) - pos;
        printf("XXXXXXX extrabits=%ld pos=%ld\n",extrabits,pos);
    }*/

    s = codes_power<double>(binary_scale_factor, 2);
    d = codes_power<double>(-decimal_scale_factor, 10);
    for (i = 0; i < numberOfSecondOrderPackedValues; i++) {
        values[i] = (double)(((X[i] * s) + reference_value) * d);
    }

    *len = numberOfSecondOrderPackedValues;
    grib_context_free(context_, secondaryBitmap);
    grib_context_free(context_, firstOrderValues);
    grib_context_free(context_, X);

    return ret;
}

int grib_accessor_data_g1second_order_constant_width_packing_t::pack_double(const double* cval, size_t* len)
{
    grib_context_log(context_, GRIB_LOG_ERROR, "%s: %s: Not implemented", class_name_, __func__);
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_data_g1second_order_constant_width_packing_t::unpack_double_element(size_t idx, double* val)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    size_t size       = 0;
    double* values    = NULL;
    int err           = 0;

    /* TODO: This should be 'codedValues' not 'values'
       but GRIB1 version of this packing does not have that key!! */
    err = grib_get_size(hand, "values", &size);
    if (err)
        return err;
    if (idx > size)
        return GRIB_INVALID_ARGUMENT;

    values = (double*)grib_context_malloc_clear(context_, size * sizeof(double));
    err    = grib_get_double_array(hand, "values", values, &size);
    if (err) {
        grib_context_free(context_, values);
        return err;
    }
    *val = values[idx];
    grib_context_free(context_, values);
    return GRIB_SUCCESS;
}

int grib_accessor_data_g1second_order_constant_width_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    size_t size = 0, i = 0;
    double* values = NULL;
    int err        = 0;

    /* TODO: This should be 'codedValues' not 'values'
       but GRIB1 version of this packing does not have that key!! */
    err = grib_get_size(hand, "values", &size);
    if (err) return err;

    for (i = 0; i < len; i++) {
        if (index_array[i] > size) return GRIB_INVALID_ARGUMENT;
    }

    values = (double*)grib_context_malloc_clear(context_, size * sizeof(double));
    err    = grib_get_double_array(hand, "values", values, &size);
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
