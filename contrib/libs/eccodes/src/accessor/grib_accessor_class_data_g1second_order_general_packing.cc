/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g1second_order_general_packing.h"

grib_accessor_data_g1second_order_general_packing_t _grib_accessor_data_g1second_order_general_packing{};
grib_accessor* grib_accessor_data_g1second_order_general_packing = &_grib_accessor_data_g1second_order_general_packing;

void grib_accessor_data_g1second_order_general_packing_t::init(const long v, grib_arguments* args)
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
    groupWidths_                     = args->get_name(hand, carg_++);
    edition_                         = 1;
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_g1second_order_general_packing_t::value_count(long* numberOfSecondOrderPackedValues)
{
    *numberOfSecondOrderPackedValues = 0;

    int err = grib_get_long_internal(grib_handle_of_accessor(this), numberOfSecondOrderPackedValues_, numberOfSecondOrderPackedValues);

    return err;
}

template <typename T>
int grib_accessor_data_g1second_order_general_packing_t::unpack_real(T* values, size_t* len)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");
    int ret = 0;
    long numberOfGroups, numberOfSecondOrderPackedValues;
    long* groupWidths            = 0;
    long* firstOrderValues       = 0;
    long* X                      = 0;
    long pos                     = 0;
    long widthOfFirstOrderValues = 0;
    unsigned char* buf           = (unsigned char*)grib_handle_of_accessor(this)->buffer->data;
    long i, n;
    double reference_value;
    long binary_scale_factor;
    long decimal_scale_factor;
    double s, d;
    long* secondaryBitmap;
    long groupLength, j;
    size_t groupWidthsSize;

    buf += byte_offset();
    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfGroups_, &numberOfGroups)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), widthOfFirstOrderValues_, &widthOfFirstOrderValues)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(grib_handle_of_accessor(this), reference_value_, &reference_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), numberOfSecondOrderPackedValues_,
                                      &numberOfSecondOrderPackedValues)) != GRIB_SUCCESS)
        return ret;

    if (*len < (size_t)numberOfSecondOrderPackedValues)
        return GRIB_ARRAY_TOO_SMALL;

    groupWidths     = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    groupWidthsSize = numberOfGroups;
    if ((ret = grib_get_long_array_internal(grib_handle_of_accessor(this), groupWidths_, groupWidths, &groupWidthsSize)) != GRIB_SUCCESS)
        return ret;

    secondaryBitmap                                  = (long*)grib_context_malloc_clear(context_, sizeof(long) * (numberOfSecondOrderPackedValues + 1));
    secondaryBitmap[numberOfSecondOrderPackedValues] = 1;
    grib_decode_long_array(buf, &pos, 1, numberOfSecondOrderPackedValues, secondaryBitmap);
    pos = 8 * ((pos + 7) / 8);

    firstOrderValues = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    grib_decode_long_array(buf, &pos, widthOfFirstOrderValues, numberOfGroups, firstOrderValues);
    pos = 8 * ((pos + 7) / 8);

    X = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfSecondOrderPackedValues);

    n           = 0;
    i           = -1;
    groupLength = 0;
    while (n < numberOfSecondOrderPackedValues) {
        if (secondaryBitmap[n]) {
            long* p     = secondaryBitmap + n + 1;
            groupLength = 1;
            while (*p != 1) {
                groupLength++;
                p++;
            }
            i++;
        }
        if (groupWidths[i] > 0) {
            for (j = 0; j < groupLength; j++) {
                X[n] = grib_decode_unsigned_long(buf, &pos, groupWidths[i]);
                X[n] = firstOrderValues[i] + X[n];
                n++;
            }
        }
        else {
            for (j = 0; j < groupLength; j++) {
                X[n] = firstOrderValues[i];
                n++;
            }
        }
    }

    s = codes_power<T>(binary_scale_factor, 2);
    d = codes_power<T>(-decimal_scale_factor, 10);
    for (i = 0; i < numberOfSecondOrderPackedValues; i++) {
        values[i] = (T)(((X[i] * s) + reference_value) * d);
    }

    *len = numberOfSecondOrderPackedValues;
    grib_context_free(context_, secondaryBitmap);
    grib_context_free(context_, firstOrderValues);
    grib_context_free(context_, X);
    grib_context_free(context_, groupWidths);

    return ret;
}

int grib_accessor_data_g1second_order_general_packing_t::unpack_float(float* values, size_t* len)
{
    return unpack_real<float>(values, len);
}

int grib_accessor_data_g1second_order_general_packing_t::unpack_double(double* values, size_t* len)
{
    return unpack_real<double>(values, len);
}

int grib_accessor_data_g1second_order_general_packing_t::pack_double(const double* cval, size_t* len)
{
    /* return GRIB_NOT_IMPLEMENTED; */
    char type[]       = "grid_second_order";
    size_t size       = strlen(type);
    grib_handle* hand = grib_handle_of_accessor(this);

    int err = grib_set_string(hand, "packingType", type, &size);
    if (err)
        return err;

    return grib_set_double_array(hand, "values", cval, *len);
}
