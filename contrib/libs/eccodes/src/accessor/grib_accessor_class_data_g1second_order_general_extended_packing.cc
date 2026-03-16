/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g1second_order_general_extended_packing.h"
#include "grib_scaling.h"

grib_accessor_data_g1second_order_general_extended_packing_t _grib_accessor_data_g1second_order_general_extended_packing{};
grib_accessor* grib_accessor_data_g1second_order_general_extended_packing = &_grib_accessor_data_g1second_order_general_extended_packing;

#define MAX_NUMBER_OF_GROUPS 65534
#define EFDEBUG              0

static const unsigned long nbits[64] = {
    0x1, 0x2, 0x4, 0x8,
    0x10, 0x20, 0x40, 0x80,
    0x100, 0x200, 0x400, 0x800,
    0x1000, 0x2000, 0x4000, 0x8000,
    0x10000, 0x20000, 0x40000, 0x80000,
    0x100000, 0x200000, 0x400000, 0x800000,
    0x1000000, 0x2000000, 0x4000000, 0x8000000,
    0x10000000, 0x20000000, 0x40000000, 0x80000000,
    0x100000000, 0x200000000, 0x400000000, 0x800000000,
    0x1000000000, 0x2000000000, 0x4000000000, 0x8000000000,
    0x10000000000, 0x20000000000, 0x40000000000, 0x80000000000,
    0x100000000000, 0x200000000000, 0x400000000000, 0x800000000000,
    0x1000000000000, 0x2000000000000, 0x4000000000000, 0x8000000000000,
    0x10000000000000, 0x20000000000000, 0x40000000000000, 0x80000000000000,
    0x100000000000000, 0x200000000000000, 0x400000000000000, 0x800000000000000,
    0x1000000000000000, 0x2000000000000000, 0x4000000000000000, 0x8000000000000000
};

long number_of_bits(grib_handle* h, unsigned long x)
{
    const unsigned long* n = nbits;
    const int count        = sizeof(nbits) / sizeof(nbits[0]);
    long i                 = 0;
    while (x >= *n) {
        n++;
        i++;
        if (i >= count) {
            /*h->dump_content(stdout,"debug", ~0, NULL);*/
            grib_context_log(h->context, GRIB_LOG_FATAL,
                             "grib_accessor_data_g1second_order_general_extended_packing: Number out of range: %ld", x);
        }
    }
    return i;
}

void grib_accessor_data_g1second_order_general_extended_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);
    grib_handle* handle = grib_handle_of_accessor(this);

    half_byte_                       = args->get_name(handle, carg_++);
    packingType_                     = args->get_name(handle, carg_++);
    ieee_packing_                    = args->get_name(handle, carg_++);
    precision_                       = args->get_name(handle, carg_++);
    widthOfFirstOrderValues_         = args->get_name(handle, carg_++);
    firstOrderValues_                = args->get_name(handle, carg_++);
    N1_                              = args->get_name(handle, carg_++);
    N2_                              = args->get_name(handle, carg_++);
    numberOfGroups_                  = args->get_name(handle, carg_++);
    codedNumberOfGroups_             = args->get_name(handle, carg_++);
    numberOfSecondOrderPackedValues_ = args->get_name(handle, carg_++);
    extraValues_                     = args->get_name(handle, carg_++);
    groupWidths_                     = args->get_name(handle, carg_++);
    widthOfWidths_                   = args->get_name(handle, carg_++);
    groupLengths_                    = args->get_name(handle, carg_++);
    widthOfLengths_                  = args->get_name(handle, carg_++);
    NL_                              = args->get_name(handle, carg_++);
    SPD_                             = args->get_name(handle, carg_++);
    widthOfSPD_                      = args->get_name(handle, carg_++);
    orderOfSPD_                      = args->get_name(handle, carg_++);
    numberOfPoints_                  = args->get_name(handle, carg_++);
    dataFlag_                        = args->get_name(handle, carg_++);
    edition_                         = 1;
    dirty_                           = 1;
    dvalues_                         = NULL;
    fvalues_                         = NULL;
    double_dirty_ = float_dirty_ = 1;
    size_                        = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_g1second_order_general_extended_packing_t::value_count(long* count)
{
    long numberOfCodedValues = 0;
    long numberOfGroups      = 0;
    size_t ngroups;
    long* groupLengths;
    long orderOfSPD = 0;
    long i;
    int err = 0;

    *count = 0;

    err = grib_get_long(grib_handle_of_accessor(this), numberOfGroups_, &numberOfGroups);
    if (err)
        return err;
    if (numberOfGroups == 0)
        return 0;

    groupLengths = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    ngroups      = numberOfGroups;
    err          = grib_get_long_array(grib_handle_of_accessor(this), groupLengths_, groupLengths, &ngroups);
    if (err)
        return err;

    for (i = 0; i < numberOfGroups; i++)
        numberOfCodedValues += groupLengths[i];

    grib_context_free(context_, groupLengths);

    err = grib_get_long(grib_handle_of_accessor(this), orderOfSPD_, &orderOfSPD);

    *count = numberOfCodedValues + orderOfSPD;

    return err;
}

int grib_accessor_data_g1second_order_general_extended_packing_t::unpack_double_element(size_t idx, double* val)
{
    size_t size;
    double* values;
    int err = 0;

    /* GRIB-564: The index idx relates to codedValues NOT values! */
    err = grib_get_size(grib_handle_of_accessor(this), "codedValues", &size);
    if (err)
        return err;
    if (idx >= size)
        return GRIB_INVALID_ARGUMENT;

    values = (double*)grib_context_malloc_clear(context_, size * sizeof(double));
    err    = grib_get_double_array(grib_handle_of_accessor(this), "codedValues", values, &size);
    if (err) {
        grib_context_free(context_, values);
        return err;
    }
    *val = values[idx];
    grib_context_free(context_, values);
    return GRIB_SUCCESS;
}

int grib_accessor_data_g1second_order_general_extended_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
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

int grib_accessor_data_g1second_order_general_extended_packing_t::unpack(double* dvalues, float* fvalues, size_t* len)
{
    int ret = 0;
    long numberOfGroups, numberOfSecondOrderPackedValues;
    long* firstOrderValues = 0;
    long* X                = 0;
    long pos               = 0;
    grib_handle* handle    = grib_handle_of_accessor(this);
    unsigned char* buf     = (unsigned char*)handle->buffer->data;
    long i, n;
    double reference_value;
    long binary_scale_factor;
    long decimal_scale_factor;
    long j;
    // long count = 0;
    long *groupWidths = NULL, *groupLengths = NULL;
    long orderOfSPD     = 0;
    long* SPD           = 0;
    long numberOfValues = 0;
    long bias           = 0;
    long y = 0, z = 0, w = 0;
    size_t k, ngroups;
    ECCODES_ASSERT(!(dvalues && fvalues));

    if (dvalues) {
        if (!double_dirty_) {
            if (*len < size_) {
                return GRIB_ARRAY_TOO_SMALL;
            }
            for (k = 0; k < size_; k++)
                dvalues[k] = dvalues_[k];
            *len = size_;
            return GRIB_SUCCESS;
        }
        double_dirty_ = 0;
    }

    if (fvalues) {
        if (!float_dirty_) {
            if (*len < size_) {
                return GRIB_ARRAY_TOO_SMALL;
            }
            for (k = 0; k < size_; k++)
                fvalues[k] = fvalues_[k];
            *len = size_;
            return GRIB_SUCCESS;
        }
        float_dirty_ = 0;
    }

    buf += byte_offset();
    ret = value_count(&numberOfValues);

    if (ret)
        return ret;

    if (*len < (size_t)numberOfValues)
        return GRIB_ARRAY_TOO_SMALL;

    if ((ret = grib_get_long_internal(handle, numberOfGroups_, &numberOfGroups)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return ret;

    ngroups     = numberOfGroups;
    groupWidths = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    ret         = grib_get_long_array(handle, groupWidths_, groupWidths, &ngroups);
    if (ret != GRIB_SUCCESS)
        return ret;

    groupLengths = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    ret          = grib_get_long_array(handle, groupLengths_, groupLengths, &ngroups);
    if (ret != GRIB_SUCCESS)
        return ret;

    firstOrderValues = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
    ret              = grib_get_long_array(handle, firstOrderValues_, firstOrderValues, &ngroups);
    if (ret != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(handle, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, numberOfSecondOrderPackedValues_,
                                      &numberOfSecondOrderPackedValues)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, orderOfSPD_, &orderOfSPD)) != GRIB_SUCCESS)
        return ret;

    if (orderOfSPD) {
        size_t nSPD = orderOfSPD + 1;
        SPD         = (long*)grib_context_malloc_clear(context_, sizeof(long) * nSPD);
        ret         = grib_get_long_array(handle, SPD_, SPD, &nSPD);
        bias        = SPD[orderOfSPD];
        if (ret != GRIB_SUCCESS)
            return ret;
    }

    X = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfValues);

    n = orderOfSPD;
    for (i = 0; i < numberOfGroups; i++) {
        if (groupWidths[i] > 0) {
            grib_decode_long_array(buf, &pos, groupWidths[i], groupLengths[i],
                                   &X[n]);
            for (j = 0; j < groupLengths[i]; j++) {
                X[n] += firstOrderValues[i];
                // count++;
                n++;
            }

            //             for (j=0;j<groupLengths[i];j++) {
            //                 X[n]=grib_decode_unsigned_long(buf,&pos,groupWidths[i]);
            //                 //printf("DXXXXX %ld %ld %ld %ld\n",n,X[n],groupWidths[i],groupLengths[i]);
            //                 X[n]+=firstOrderValues[i];
            //                 count++;
            //                 n++;
            //             }
        }
        else {
            for (j = 0; j < groupLengths[i]; j++) {
                X[n] = firstOrderValues[i];
                n++;
            }
        }
    }

    for (i = 0; i < orderOfSPD; i++) {
        X[i] = SPD[i];
    }

    switch (orderOfSPD) {
        case 1:
            y = X[0];
            for (i = 1; i < numberOfValues; i++) {
                y += X[i] + bias;
                X[i] = y;
            }

            break;
        case 2:
            y = X[1] - X[0];
            z = X[1];
            for (i = 2; i < numberOfValues; i++) {
                y += X[i] + bias;
                z += y;
                X[i] = z;
            }

            break;
        case 3:
            y = X[2] - X[1];
            z = y - (X[1] - X[0]);
            w = X[2];
            for (i = 3; i < numberOfValues; i++) {
                z += X[i] + bias;
                y += z;
                w += y;
                X[i] = w;
            }
            break;
    }

    if (dvalues) {  // double-precision
        if (dvalues_) {
            if (numberOfValues != size_) {
                grib_context_free(context_, dvalues_);
                dvalues_ = (double*)grib_context_malloc_clear(context_, sizeof(double) * numberOfValues);
            }
        }
        else {
            dvalues_ = (double*)grib_context_malloc_clear(context_, sizeof(double) * numberOfValues);
        }

        double s = codes_power<double>(binary_scale_factor, 2);
        double d = codes_power<double>(-decimal_scale_factor, 10);
        for (i = 0; i < numberOfValues; i++) {
            dvalues[i]  = (double)(((X[i] * s) + reference_value) * d);
            dvalues_[i] = dvalues[i];
        }
    }
    else {
        // single-precision
        if (fvalues_) {
            if (numberOfValues != size_) {
                grib_context_free(context_, fvalues_);
                fvalues_ = (float*)grib_context_malloc_clear(context_, sizeof(float) * numberOfValues);
            }
        }
        else {
            fvalues_ = (float*)grib_context_malloc_clear(context_, sizeof(float) * numberOfValues);
        }

        float s = codes_power<float>(binary_scale_factor, 2);
        float d = codes_power<float>(-decimal_scale_factor, 10);
        for (i = 0; i < numberOfValues; i++) {
            fvalues[i]  = (float)(((X[i] * s) + reference_value) * d);
            fvalues_[i] = fvalues[i];
        }
    }

    *len  = numberOfValues;
    size_ = numberOfValues;

    grib_context_free(context_, X);
    grib_context_free(context_, groupWidths);
    grib_context_free(context_, groupLengths);
    grib_context_free(context_, firstOrderValues);
    if (orderOfSPD)
        grib_context_free(context_, SPD);

    return ret;
}

int grib_accessor_data_g1second_order_general_extended_packing_t::unpack_float(float* values, size_t* len)
{
    return unpack(NULL, values, len);
}

int grib_accessor_data_g1second_order_general_extended_packing_t::unpack_double(double* values, size_t* len)
{
    return unpack(values, NULL, len);
}

static void grib_split_long_groups(grib_handle* hand, grib_context* c, long* numberOfGroups, long* lengthOfSecondOrderValues,
                                   long* groupLengths, long* widthOfLengths,
                                   long* groupWidths, long widthOfWidths,
                                   long* firstOrderValues, long widthOfFirstOrderValues)
{
    long i, j;
    long newWidth, delta;
    long* widthsOfLengths;
    long* localWidthsOfLengths;
    long* localLengths;
    long* localWidths;
    long* localFirstOrderValues;
    int maxNumberOfGroups = *numberOfGroups * 2;

    /* the widthOfLengths is the same for all the groupLengths and therefore if
        few big groups are present all the groups have to be coded with a large number
        of bits (big widthOfLengths) even if the majority of them is small.
        Here we try to reduce the size of the message splitting the big groups.
     */

    widthsOfLengths = (long*)grib_context_malloc_clear(c, sizeof(long) * maxNumberOfGroups);
    j               = 0;
    /* compute the widthOfLengths and the number of big groups */
    for (i = 0; i < *numberOfGroups; i++) {
        widthsOfLengths[i] = number_of_bits(hand, groupLengths[i]);
        if (*widthOfLengths == widthsOfLengths[i]) {
            j++;
        }
    }

    /* variation of the size of message due to decrease of groupLengths
       of 1*/
    newWidth = *widthOfLengths - 1;
    delta    = j * (widthOfWidths + widthOfFirstOrderValues + newWidth) - *numberOfGroups;

    if (delta >= 0) {
        grib_context_free(c, widthsOfLengths);
        return;
    }

    localWidthsOfLengths  = (long*)grib_context_malloc_clear(c, sizeof(long) * maxNumberOfGroups);
    localLengths          = (long*)grib_context_malloc_clear(c, sizeof(long) * maxNumberOfGroups);
    localWidths           = (long*)grib_context_malloc_clear(c, sizeof(long) * maxNumberOfGroups);
    localFirstOrderValues = (long*)grib_context_malloc_clear(c, sizeof(long) * maxNumberOfGroups);

    while (newWidth > 0) {
        /* it is worth to split big groups */
        j = 0;
        for (i = 0; i < *numberOfGroups; i++) {
            if (newWidth < widthsOfLengths[i]) {
                localLengths[j]          = groupLengths[i] / 2;
                localWidthsOfLengths[j]  = number_of_bits(hand, localLengths[j]);
                localWidths[j]           = groupWidths[i];
                localFirstOrderValues[j] = firstOrderValues[i];
                j++;
                localLengths[j]          = groupLengths[i] - localLengths[j - 1];
                localWidthsOfLengths[j]  = number_of_bits(hand, localLengths[j]);
                localWidths[j]           = groupWidths[i];
                localFirstOrderValues[j] = firstOrderValues[i];
                if (localWidthsOfLengths[j] > newWidth) {
                    localLengths[j]--;
                    localWidthsOfLengths[j]--;
                    j++;
                    localLengths[j]          = 1;
                    localWidthsOfLengths[j]  = 1;
                    localWidths[j]           = groupWidths[i];
                    localFirstOrderValues[j] = firstOrderValues[i];
                }
                j++;
            }
            else {
                localLengths[j]          = groupLengths[i];
                localWidthsOfLengths[j]  = widthsOfLengths[i];
                localWidths[j]           = groupWidths[i];
                localFirstOrderValues[j] = firstOrderValues[i];
                j++;
            }
        }

        if (j > maxNumberOfGroups)
            break;

        *numberOfGroups            = j;
        *widthOfLengths            = newWidth;
        j                          = 0;
        *lengthOfSecondOrderValues = 0;
        for (i = 0; i < *numberOfGroups; i++) {
            groupLengths[i]     = localLengths[i];
            widthsOfLengths[i]  = localWidthsOfLengths[i];
            groupWidths[i]      = localWidths[i];
            firstOrderValues[i] = localFirstOrderValues[i];
            *lengthOfSecondOrderValues += groupLengths[i] * groupWidths[i];
            if (*widthOfLengths == widthsOfLengths[i])
                j++;
        }

        newWidth--;
        delta = j * (widthOfWidths + widthOfFirstOrderValues + newWidth) - *numberOfGroups;
        if (delta >= 0)
            break;
    }

    grib_context_free(c, widthsOfLengths);
    grib_context_free(c, localWidthsOfLengths);
    grib_context_free(c, localLengths);
    grib_context_free(c, localWidths);
    grib_context_free(c, localFirstOrderValues);
}

static int get_bits_per_value(grib_handle* h, const char* bits_per_value_str, long* bits_per_value)
{
    int err = 0;
    if ((err = grib_get_long_internal(h, bits_per_value_str, bits_per_value)) != GRIB_SUCCESS)
        return err;

    if (*bits_per_value == 0) {
        /* Probably grid_ieee input which is a special case. Note: we cannot check the packingType
         * because it has already been changed to second order!
         * We have to take precision=1 for IEEE which is 32bits
         */
        /* Note: on a 32bit system, the most significant bit is for signedness, so we have to drop one bit */
        if (sizeof(long) == 4) {
            *bits_per_value = 31;
        }
        else {
            *bits_per_value = 32;
        }
    }
    return err;
}

// For the old implementation of pack_double, see
//  src/deprecated/grib_accessor_data_g1second_order_general_extended_packing.pack_double.cc
// See ECC-441 and ECC-261
int grib_accessor_data_g1second_order_general_extended_packing_t::pack_double(const double* val, size_t* len)
{
    int ret   = 0;
    int grib2 = 0;
    long bits_per_value, orderOfSPD, binary_scale_factor;
    long numberOfValues;
    double max, min;
    double decimal, divisor;
    double reference_value;
    size_t size, sizebits;
    long half_byte;
    long* X;
    long* Xp;
    long i;
    long incrementGroupLengthA, groupWidthA, prevGroupLength, offsetD, remainingValuesB, groupLengthB;
    long maxB, minB, maxAB, minAB;
    long offsetBeforeData, offsetSection4;
    unsigned char* buffer = NULL;
    long maxWidth, maxLength, widthOfWidths, NL, widthOfLengths, N1, N2, extraValues, codedNumberOfGroups, numberOfSecondOrderPackedValues;
    long pos;

    long numberOfGroups;
    long groupLengthC, groupLengthA, remainingValues, count;
    long maxA = 0, minA = 0;
    long maxC, minC, offsetC;
    long maxAC, minAC;
    long range, bias = 0, maxSPD;
    long firstOrderValuesMax, offset, groupLength, j, groupWidth, firstOrderValue, lengthOfSecondOrderValues;
    long *groupLengths, *groupWidths, *firstOrderValues;
    /* long groupLengths[MAX_NUMBER_OF_GROUPS],groupWidths[MAX_NUMBER_OF_GROUPS],firstOrderValues[MAX_NUMBER_OF_GROUPS]; */

    /* TODO put these parameters in def file */
    long startGroupLength     = 15;
    long incrementGroupLength = 3;
    long minGroupLength       = 3;
    long widthOfSPD = 0, widthOfBias = 0;
    long* offsets;
    long widthOfFirstOrderValues;
    int computeGroupA = 1;
    long dataHeadersLength, widthsLength, lengthsLength, firstOrderValuesLength;
    long decimal_scale_factor;
    grib_handle* handle          = grib_handle_of_accessor(this);
    long optimize_scaling_factor = 0;

    numberOfValues = *len;

    min = max = val[0];
    for (i = 1; i < numberOfValues; i++) {
        if (val[i] > max)
            max = val[i];
        else if (val[i] < min)
            min = val[i];
    }
    if ((ret = grib_check_data_values_minmax(handle, min, max)) != GRIB_SUCCESS) {
        return ret;
    }

    /* ECC-1219: packingType conversion from grid_ieee to grid_second_order */
    if ((ret = get_bits_per_value(handle, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, optimize_scaling_factor_, &optimize_scaling_factor)) != GRIB_SUCCESS)
        return ret;

    // ECC-1986: Make sure we set the dirty flag after calling get_bits_per_value
    double_dirty_ = 1;

    if (optimize_scaling_factor) {
        const int compat_gribex = handle->context->gribex_mode_on && edition_ == 1;
        const int compat_32bit  = 1;
        if ((ret = grib_optimize_decimal_factor(this, reference_value_,
                                                max, min, bits_per_value,
                                                compat_gribex, compat_32bit,
                                                &decimal_scale_factor, &binary_scale_factor, &reference_value)) != GRIB_SUCCESS)
            return ret;

        decimal = codes_power<double>(decimal_scale_factor, 10);
        divisor = codes_power<double>(-binary_scale_factor, 2);
        /*min     = min * decimal;*/
        /*max     = max * decimal;*/

        if ((ret = grib_set_long_internal(handle, decimal_scale_factor_, decimal_scale_factor)) !=
            GRIB_SUCCESS)
            return ret;
    }
    else {
        /* For constant fields set decimal scale factor to 0 (See GRIB-165) */
        if (min == max) {
            grib_set_long_internal(handle, decimal_scale_factor_, 0);
        }

        if ((ret = grib_get_long_internal(handle, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
            return ret;

        decimal = codes_power<double>(decimal_scale_factor, 10);
        min     = min * decimal;
        max     = max * decimal;

        if (grib_get_nearest_smaller_value(handle, reference_value_, min, &reference_value) != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "Unable to find nearest_smaller_value of %g for %s", min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }
        binary_scale_factor = grib_get_binary_scale_fact(max, reference_value, bits_per_value, &ret);
        if (ret != GRIB_SUCCESS) return ret;

        divisor = codes_power<double>(-binary_scale_factor, 2);
    }

    if ((ret = grib_set_long_internal(handle, binary_scale_factor_, binary_scale_factor)) !=
        GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_double_internal(handle, reference_value_, reference_value)) !=
        GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, offsetdata_, &offsetBeforeData)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, offsetsection_, &offsetSection4)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(handle, orderOfSPD_, &orderOfSPD)) != GRIB_SUCCESS)
        return ret;

    X = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfValues);
    for (i = 0; i < numberOfValues; i++) {
        X[i] = (((val[i] * decimal) - reference_value) * divisor) + 0.5;
    }

    groupLengths     = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfValues);
    groupWidths      = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfValues);
    firstOrderValues = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfValues);

    /* spatial differencing */
    switch (orderOfSPD) {
        case 1:
            for (i = numberOfValues - 1; i > 0; i--) {
                X[i] -= X[i - 1];
            }
            break;
        case 2:
            for (i = numberOfValues - 1; i > 1; i--) {
                X[i] -= 2 * X[i - 1] - X[i - 2];
            }
            break;
        case 3:
            for (i = numberOfValues - 1; i > 2; i--) {
                X[i] -= 3 * (X[i - 1] - X[i - 2]) + X[i - 3];
            }
            break;
    }
    if (orderOfSPD) {
        ECCODES_ASSERT(orderOfSPD >= 0 && orderOfSPD < numberOfValues);
        bias = X[orderOfSPD];
        for (i = orderOfSPD + 1; i < numberOfValues; i++) {
            if (bias > X[i])
                bias = X[i];
        }
        for (i = orderOfSPD; i < numberOfValues; i++) {
            X[i] -= bias;
        }
        maxSPD = X[0];
        for (i = 1; i < orderOfSPD; i++) {
            if (maxSPD < X[i])
                maxSPD = X[i];
        }
        /* widthOfSPD=(long)ceil(log((double)(maxSPD+1))/log(2.0)); */
        widthOfSPD  = number_of_bits(handle, maxSPD);
        widthOfBias = number_of_bits(handle, labs(bias)) + 1;

        if (widthOfSPD < widthOfBias)
            widthOfSPD = widthOfBias;
    }
    /* end of spatial differencing */

    count                 = orderOfSPD;
    remainingValues       = numberOfValues - count;
    numberOfGroups        = 0;
    incrementGroupLengthA = startGroupLength;

    computeGroupA = 1;
    while (remainingValues) {
        /* group A created with length=incrementGroupLengthA (if enough values remain)
           incrementGroupLengthA=startGroupLength always except when coming from an A+C or A+B ok branch
         */
        groupLengthA = incrementGroupLengthA < remainingValues ? incrementGroupLengthA : remainingValues;
        if (computeGroupA) {
            maxA = X[count];
            minA = X[count];
            for (i = 1; i < groupLengthA; i++) {
                DEBUG_ASSERT_ACCESS(X, count + i, numberOfValues);
                if (maxA < X[count + i])
                    maxA = X[count + i];
                if (minA > X[count + i])
                    minA = X[count + i];
            }
        }
        groupWidthA = number_of_bits(handle, maxA - minA);
        range       = (long)codes_power<double>(groupWidthA, 2) - 1;

        offsetC = count + groupLengthA;
        if (offsetC == numberOfValues) {
            /* no more values close group A and end loop */
            groupLengths[numberOfGroups] = groupLengthA;
            groupWidths[numberOfGroups]  = groupWidthA;
            /* firstOrderValues[numberOfGroups]=minA; */
            /* to optimise the width of first order variable */
            firstOrderValues[numberOfGroups] = maxA - range > 0 ? maxA - range : 0;
            numberOfGroups++;
            break;
        }

        /* group C created with length=incrementGroupLength (fixed)
           or remaining values if close to end
         */
        groupLengthC = incrementGroupLength;
        if (groupLengthC + offsetC > numberOfValues - startGroupLength / 2) {
            groupLengthC = numberOfValues - offsetC;
        }
        maxC = X[offsetC];
        minC = X[offsetC];
        for (i = 1; i < groupLengthC; i++) {
            DEBUG_ASSERT_ACCESS(X, offsetC + i, numberOfValues);
            if (maxC < X[offsetC + i])
                maxC = X[offsetC + i];
            if (minC > X[offsetC + i])
                minC = X[offsetC + i];
        }

        maxAC = maxA > maxC ? maxA : maxC;
        minAC = minA < minC ? minA : minC;

        /* check if A+C can be represented with the same width as A*/
        if (maxAC - minAC > range) {
            /* A could not be expanded adding C. Check if A could be expanded taking
               some elements from preceding group. The condition is always that width of
               A doesn't increase.
             */
            if (numberOfGroups > 0 && groupWidths[numberOfGroups - 1] > groupWidthA) {
                prevGroupLength = groupLengths[numberOfGroups - 1] - incrementGroupLength;
                offsetC         = count - incrementGroupLength;
                /* preceding group length cannot be less than a minimum value */
                while (prevGroupLength >= minGroupLength) {
                    maxAC = maxA;
                    minAC = minA;
                    for (i = 0; i < incrementGroupLength; i++) {
                        if (maxAC < X[offsetC + i])
                            maxAC = X[offsetC + i];
                        if (minAC > X[offsetC + i])
                            minAC = X[offsetC + i];
                    }

                    /* no more elements can be transfered, exit loop*/
                    if (maxAC - minAC > range)
                        break;

                    maxA = maxAC;
                    minA = minAC;
                    groupLengths[numberOfGroups - 1] -= incrementGroupLength;
                    groupLengthA += incrementGroupLength;
                    count -= incrementGroupLength;
                    remainingValues += incrementGroupLength;

                    offsetC -= incrementGroupLength;
                    prevGroupLength -= incrementGroupLength;
                }
            }
            /* close group A*/
            groupLengths[numberOfGroups] = groupLengthA;
            groupWidths[numberOfGroups]  = groupWidthA;
            /* firstOrderValues[numberOfGroups]=minA; */
            /* to optimise the width of first order variable */
            firstOrderValues[numberOfGroups] = maxA - range > 0 ? maxA - range : 0;
            count += groupLengthA;
            remainingValues -= groupLengthA;
            numberOfGroups++;
            /* incrementGroupLengthA is reset to the fixed startGroupLength as it
               could have been changed after the A+C or A+B ok condition.
             */
            incrementGroupLengthA = startGroupLength;
            computeGroupA         = 1;

            //             if (numberOfGroups==MAX_NUMBER_OF_GROUPS) {
            //                 groupLengthA= remainingValues ;
            //                 maxA=X[count];
            //                 minA=X[count];
            //                 for (i=1;i<groupLengthA;i++) {
            //                     if (maxA<X[count+i]) maxA=X[count+i];
            //                     if (minA>X[count+i]) minA=X[count+i];
            //                 }
            //                 groupWidthA=number_of_bits(maxA-minA);
            //                 range=(long)codes_power<double>(groupWidthA,2)-1;
            //                 groupLengths[numberOfGroups]=groupLengthA;
            //                 groupWidths[numberOfGroups]=groupWidthA;
            //                 firstOrderValues[numberOfGroups] = maxA-range > 0 ? maxA-range : 0;
            //                 break;
            //             }

            continue;
        }

        /* A+C could be coded with the same width as A*/
        offsetD = offsetC + groupLengthC;
        if (offsetD == numberOfValues) {
            groupLengths[numberOfGroups] = groupLengthA + groupLengthC;
            groupWidths[numberOfGroups]  = groupWidthA;

            /* range of AC is the same as A*/
            /* firstOrderValues[numberOfGroups]=minAC; */
            /* to optimise the width of first order variable */
            firstOrderValues[numberOfGroups] = maxAC - range > 0 ? maxAC - range : 0;
            numberOfGroups++;
            break;
        }

        /* group B is created with length startGroupLength, starting at the
           same offset as C.
         */
        remainingValuesB = numberOfValues - offsetC;
        groupLengthB     = startGroupLength < remainingValuesB ? startGroupLength : remainingValuesB;
        maxB             = maxC;
        minB             = minC;
        for (i = groupLengthC; i < groupLengthB; i++) {
            if (maxB < X[offsetC + i])
                maxB = X[offsetC + i];
            if (minB > X[offsetC + i])
                minB = X[offsetC + i];
        }

        /* check if group B can be coded with a smaller width than A */
        if (maxB - minB <= range / 2 && range > 0) {
            /* TODO Add code to try if A can be expanded taking some elements
               from the left (preceding) group.
                A possible variation is to do this left check (and the previous one)
                in the final loop when checking that the width of each group.
             */

            /* close group A and continue loop*/
            groupLengths[numberOfGroups] = groupLengthA;
            groupWidths[numberOfGroups]  = groupWidthA;
            /* firstOrderValues[numberOfGroups]=minA; */
            /* to optimise the width of first order variable */
            firstOrderValues[numberOfGroups] = maxA - range > 0 ? maxA - range : 0;
            count += groupLengthA;
            remainingValues -= groupLengthA;
            numberOfGroups++;

            //             if (numberOfGroups==MAX_NUMBER_OF_GROUPS) {
            //                 groupLengthA= remainingValues ;
            //                 maxA=X[count];
            //                 minA=X[count];
            //                 for (i=1;i<groupLengthA;i++) {
            //                     if (maxA<X[count+i]) maxA=X[count+i];
            //                     if (minA>X[count+i]) minA=X[count+i];
            //                 }
            //                 groupWidthA=number_of_bits(maxA-minA);
            //                 range=(long)codes_power<double>(groupWidthA,2)-1;
            //                 groupLengths[numberOfGroups]=groupLengthA;
            //                 groupWidths[numberOfGroups]=groupWidthA;
            //                 firstOrderValues[numberOfGroups] = maxA-range > 0 ? maxA-range : 0;
            //                 break;
            //             }

            incrementGroupLengthA = startGroupLength;
            computeGroupA         = 1;
            continue;
        }

        /* check if A+B can be coded with same with as A */
        maxAB = maxA > maxB ? maxA : maxB;
        minAB = minA < minB ? minA : minB;
        if (maxAB - minAB <= range) {
            /* A+B can be merged. The increment used at the beginning of the loop to
               build group C is increased to the size of group B
             */
            incrementGroupLengthA += groupLengthB;
            maxA          = maxAB;
            minA          = minAB;
            computeGroupA = 0;
            continue;
        }

        /* A+B cannot be merged, A+C can be merged*/
        incrementGroupLengthA += groupLengthC;
        computeGroupA = 1;

    } /* end of the while*/

    /* computing bitsPerValue as the number of bits needed to represent
       the firstOrderValues.
     */
    max = firstOrderValues[0];
    min = firstOrderValues[0];
    for (i = 1; i < numberOfGroups; i++) {
        if (max < firstOrderValues[i])
            max = firstOrderValues[i];
        if (min > firstOrderValues[i])
            min = firstOrderValues[i];
    }
    widthOfFirstOrderValues = number_of_bits(handle, max - min);
    firstOrderValuesMax     = (long)codes_power<double>(widthOfFirstOrderValues, 2) - 1;

    if (numberOfGroups > 2) {
        /* loop through all the groups except the last in reverse order to
           check if each group width is still appropriate for the group.
           Focus on groups which have been shrank as left groups of an A group taking
           some of their elements.
         */
        offsets    = (long*)grib_context_malloc_clear(context_, sizeof(long) * numberOfGroups);
        offsets[0] = orderOfSPD;
        for (i = 1; i < numberOfGroups; i++)
            offsets[i] = offsets[i - 1] + groupLengths[i - 1];
        for (i = numberOfGroups - 2; i >= 0; i--) {
            offset      = offsets[i];
            groupLength = groupLengths[i];

            if (groupLength >= startGroupLength)
                continue;

            max = X[offset];
            min = X[offset];
            for (j = 1; j < groupLength; j++) {
                if (max < X[offset + j])
                    max = X[offset + j];
                if (min > X[offset + j])
                    min = X[offset + j];
            }
            groupWidth = number_of_bits(handle, max - min);
            range      = (long)codes_power<double>(groupWidth, 2) - 1;

            /* width of first order values has to be unchanged.*/
            for (j = groupWidth; j < groupWidths[i]; j++) {
                firstOrderValue = max > range ? max - range : 0;
                if (firstOrderValue <= firstOrderValuesMax) {
                    groupWidths[i]      = j;
                    firstOrderValues[i] = firstOrderValue;
                    break;
                }
            }

            offsetC = offset;
            /*  group width of the current group (i) can have been reduced
                and it is worth to try to expand the group to get some elements
                from the left group if it has bigger width.
             */
            if (i > 0 && (groupWidths[i - 1] > groupWidths[i])) {
                prevGroupLength = groupLengths[i - 1] - incrementGroupLength;
                offsetC -= incrementGroupLength;
                while (prevGroupLength >= minGroupLength) {
                    for (j = 0; j < incrementGroupLength; j++) {
                        if (max < X[offsetC + j])
                            max = X[offsetC + j];
                        if (min > X[offsetC + j])
                            min = X[offsetC + j];
                    }

                    /* width of first order values has to be unchanged*/
                    firstOrderValue = max > range ? max - range : 0;
                    if (max - min > range || firstOrderValue > firstOrderValuesMax)
                        break;

                    groupLengths[i - 1] -= incrementGroupLength;
                    groupLengths[i] += incrementGroupLength;
                    firstOrderValues[i] = firstOrderValue;

                    offsetC -= incrementGroupLength;
                    prevGroupLength -= incrementGroupLength;
                }
            }
        }
        grib_context_free(context_, offsets);
    }

    maxWidth  = groupWidths[0];
    maxLength = groupLengths[0];
    for (i = 1; i < numberOfGroups; i++) {
        if (maxWidth < groupWidths[i])
            maxWidth = groupWidths[i];
        if (maxLength < groupLengths[i])
            maxLength = groupLengths[i];
    }

    if (maxWidth < 0 || maxLength < 0) {
        grib_context_log(parent_->h->context, GRIB_LOG_ERROR, "Cannot compute parameters for second order packing.");
        return GRIB_ENCODING_ERROR;
    }
    widthOfWidths  = number_of_bits(handle, maxWidth);
    widthOfLengths = number_of_bits(handle, maxLength);

    lengthOfSecondOrderValues = 0;
    for (i = 0; i < numberOfGroups; i++) {
        lengthOfSecondOrderValues += groupLengths[i] * groupWidths[i];
    }

    if (!context_->no_big_group_split) {
        grib_split_long_groups(handle, context_, &numberOfGroups, &lengthOfSecondOrderValues,
                               groupLengths, &widthOfLengths, groupWidths, widthOfWidths,
                               firstOrderValues, widthOfFirstOrderValues);
    }

    Xp = X + orderOfSPD;
    for (i = 0; i < numberOfGroups; i++) {
        for (j = 0; j < groupLengths[i]; j++) {
            *(Xp++) -= firstOrderValues[i];
        }
    }

    /* start writing to message */

    /* writing SPD */
    if (orderOfSPD) {
        if ((ret = grib_set_long_internal(handle, widthOfSPD_, widthOfSPD)) != GRIB_SUCCESS)
            return ret;
    }

    /* end writing SPD */
    if ((ret = grib_set_long_internal(handle, widthOfFirstOrderValues_, widthOfFirstOrderValues)) != GRIB_SUCCESS)
        return ret;

    dataHeadersLength = 25;
    if (orderOfSPD)
        dataHeadersLength += 1 + ((orderOfSPD + 1) * widthOfSPD + 7) / 8;
    widthsLength           = (widthOfWidths * numberOfGroups + 7) / 8;
    lengthsLength          = (widthOfLengths * numberOfGroups + 7) / 8;
    firstOrderValuesLength = (widthOfFirstOrderValues * numberOfGroups + 7) / 8;

    NL = widthsLength + dataHeadersLength + 1;
    N1 = NL + lengthsLength;
    N2 = N1 + firstOrderValuesLength;

    NL = NL > 65535 ? 65535 : NL;
    N2 = N2 > 65535 ? 65535 : N2;
    N1 = N1 > 65535 ? 65535 : N1;

    grib_set_long(handle, NL_, NL);
    grib_set_long(handle, N1_, N1);
    grib_set_long(handle, N2_, N2);

    if (numberOfGroups > 65535) {
        extraValues         = numberOfGroups / 65536;
        codedNumberOfGroups = numberOfGroups % 65536;
    }
    else {
        extraValues         = 0;
        codedNumberOfGroups = numberOfGroups;
    }

    /* if no extraValues key present it is a GRIB2*/
    grib2 = 0;
    if ((ret = grib_set_long(handle, extraValues_, extraValues)) != GRIB_SUCCESS) {
        codedNumberOfGroups = numberOfGroups;
        grib2               = 1;
    }

    if ((ret = grib_set_long_internal(handle, codedNumberOfGroups_, codedNumberOfGroups)) != GRIB_SUCCESS)
        return ret;

    numberOfSecondOrderPackedValues = numberOfValues - orderOfSPD;
    if (!grib2 && numberOfSecondOrderPackedValues > 65535)
        numberOfSecondOrderPackedValues = 65535;

    if ((ret = grib_set_long_internal(handle, numberOfSecondOrderPackedValues_, numberOfSecondOrderPackedValues)) != GRIB_SUCCESS)
        return ret;

    if (grib2) {
        if ((ret = grib_set_long_internal(handle, bits_per_value_, bits_per_value)) != GRIB_SUCCESS)
            return ret;
    }
    else {
        if ((ret = grib_set_long_internal(handle, bits_per_value_, 0)) != GRIB_SUCCESS)
            return ret;
    }

    if ((ret = grib_set_long_internal(handle, widthOfWidths_, widthOfWidths)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_set_long_internal(handle, widthOfLengths_, widthOfLengths)) != GRIB_SUCCESS)
        return ret;

    lengthOfSecondOrderValues = 0;
    for (i = 0; i < numberOfGroups; i++) {
        lengthOfSecondOrderValues += groupLengths[i] * groupWidths[i];
    }

    size     = (lengthOfSecondOrderValues + 7) / 8;
    sizebits = lengthOfSecondOrderValues;

    /* padding section 4 to an even number of octets */
    size      = ((size + offsetBeforeData - offsetSection4) % 2) ? size + 1 : size;
    half_byte = 8 * size - sizebits;
    if ((ret = grib_set_long_internal(handle, half_byte_, half_byte)) != GRIB_SUCCESS)
        return ret;

    buffer = (unsigned char*)grib_context_malloc_clear(context_, size);

    pos = 0;
    if (orderOfSPD) {
        long SPD[4] = {
            0,
        };
        size_t nSPD = orderOfSPD + 1;
        ECCODES_ASSERT(orderOfSPD <= 3);
        for (i = 0; i < orderOfSPD; i++)
            SPD[i] = X[i];
        SPD[orderOfSPD] = bias;
        ret             = grib_set_long_array_internal(handle, SPD_, SPD, nSPD);
        if (ret)
            return ret;
    }

    ret = grib_set_long_array_internal(handle, groupWidths_, groupWidths, (size_t)numberOfGroups);
    if (ret)
        return ret;

    ret = grib_set_long_array_internal(handle, groupLengths_, groupLengths, (size_t)numberOfGroups);
    if (ret)
        return ret;

    ret = grib_set_long_array_internal(handle, firstOrderValues_, firstOrderValues, (size_t)numberOfGroups);
    if (ret)
        return ret;

    Xp  = X + orderOfSPD;
    pos = 0;
#if EFDEBUG
    count = 0;
#endif
    for (i = 0; i < numberOfGroups; i++) {
        if (groupWidths[i] > 0) {
            for (j = 0; j < groupLengths[i]; j++) {
#if EFDEBUG
                printf("CXXXXX %ld %ld %ld %ld\n", count, *Xp, groupWidths[i], groupLengths[i]);
                count++;
#endif
                grib_encode_unsigned_longb(buffer, *(Xp++), &pos, groupWidths[i]);
            }
        }
        else {
            Xp += groupLengths[i];
#if EFDEBUG
            count += groupLengths[i];
#endif
        }
    }

    /* ECC-259: Set correct number of values */
    ret = grib_set_long_internal(parent_->h, number_of_values_, *len);
    if (ret) return ret;

    ret = grib_buffer_replace(this, buffer, size, 1, 1);
    if (ret) return ret;

    grib_context_free(context_, buffer);
    grib_context_free(context_, X);
    grib_context_free(context_, groupLengths);
    grib_context_free(context_, groupWidths);
    grib_context_free(context_, firstOrderValues);

    return GRIB_SUCCESS;
}

void grib_accessor_data_g1second_order_general_extended_packing_t::destroy(grib_context* context)
{
    if (dvalues_ != NULL) {
        grib_context_free(context, dvalues_);
        dvalues_ = NULL;
    }
    if (fvalues_ != NULL) {
        grib_context_free(context, fvalues_);
        fvalues_ = NULL;
    }
    grib_accessor_data_simple_packing_t::destroy(context);
}
