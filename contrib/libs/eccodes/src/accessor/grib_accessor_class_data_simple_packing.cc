/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_simple_packing.h"
#include "grib_optimize_decimal_factor.h"
#include "grib_bits_any_endian_simple.h"
#include <float.h>
#include <type_traits>

grib_accessor_data_simple_packing_t _grib_accessor_data_simple_packing{};
grib_accessor* grib_accessor_data_simple_packing = &_grib_accessor_data_simple_packing;

void grib_accessor_data_simple_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_values_t::init(v, args);
    grib_handle* gh          = grib_handle_of_accessor(this);
    units_factor_            = args->get_name(gh, carg_++);
    units_bias_              = args->get_name(gh, carg_++);
    changing_precision_      = args->get_name(gh, carg_++);
    number_of_values_        = args->get_name(gh, carg_++);
    bits_per_value_          = args->get_name(gh, carg_++);
    reference_value_         = args->get_name(gh, carg_++);
    binary_scale_factor_     = args->get_name(gh, carg_++);
    decimal_scale_factor_    = args->get_name(gh, carg_++);
    optimize_scaling_factor_ = args->get_name(gh, carg_++);
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
    dirty_ = 1;
}

static const unsigned long nbits[32] = {
    0x1, 0x2, 0x4, 0x8, 0x10, 0x20,
    0x40, 0x80, 0x100, 0x200, 0x400, 0x800,
    0x1000, 0x2000, 0x4000, 0x8000, 0x10000, 0x20000,
    0x40000, 0x80000, 0x100000, 0x200000, 0x400000, 0x800000,
    0x1000000, 0x2000000, 0x4000000, 0x8000000, 0x10000000, 0x20000000,
    0x40000000, 0x80000000
};

static int number_of_bits(unsigned long x, long* result)
{
    const int count        = sizeof(nbits) / sizeof(nbits[0]);
    const unsigned long* n = nbits;
    *result                = 0;
    while (x >= *n) {
        n++;
        (*result)++;
        if (*result >= count) {
            return GRIB_ENCODING_ERROR;
        }
    }
    return GRIB_SUCCESS;
}

int grib_accessor_data_simple_packing_t::value_count(long* number_of_values)
{
    *number_of_values = 0;

    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, number_of_values);
}

int grib_accessor_data_simple_packing_t::unpack_double_element(size_t idx, double* val)
{
    long n_vals     = 0;
    int err         = 0;
    grib_handle* gh = grib_handle_of_accessor(this);

    double reference_value;
    long binary_scale_factor;
    long bits_per_value;
    long decimal_scale_factor;
    unsigned char* buf = (unsigned char*)gh->buffer->data;
    double s           = 0;
    double d           = 0;
    long pos           = 0;

    err = value_count(&n_vals);
    if (err)
        return err;

    if ((err = grib_get_long_internal(gh, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;

    dirty_ = 0;

    if ((err = grib_get_double_internal(gh, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    /* Special case */
    if (bits_per_value == 0) {
        *val = reference_value;
        return GRIB_SUCCESS;
    }

    ECCODES_ASSERT(idx < n_vals);
    s = codes_power<double>(binary_scale_factor, 2);
    d = codes_power<double>(-decimal_scale_factor, 10);

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "%s: %s: creating %s, %ld values (idx=%zu)",
                     class_name_, __func__, name_, n_vals, idx);

    buf += byte_offset();
    /*ECCODES_ASSERT(((bits_per_value*n_vals)/8) < (1<<29));*/ /* See GRIB-787 */

    if (bits_per_value % 8) {
        grib_context_log(context_, GRIB_LOG_DEBUG,
                         "%s: calling outline function : bpv %ld, rv: %g, bsf: %ld, dsf: %ld ",
                         class_name_, bits_per_value, reference_value, binary_scale_factor, decimal_scale_factor);
        pos  = idx * bits_per_value;
        *val = (double)(((grib_decode_unsigned_long(buf, &pos, bits_per_value) * s) + reference_value) * d);
        /* val[i] = grib_decode_unsigned_long(buf, &pos, bits_per_value); */
        /* fprintf(stdout,"unpck uuu-o: %d vals %d bitspv buf %d by long \n", n_vals, bits_per_value, pos/8);*/
    }
    else {
        int bc       = 0;
        size_t octet = 0;
        long lvalue  = 0;
        int l        = bits_per_value / 8;

        pos = idx * l;
        buf += pos;
        lvalue |= buf[octet++];

        for (bc = 1; bc < l; bc++) {
            lvalue <<= 8;
            lvalue |= buf[octet++];
        }
        *val = (double)(((lvalue * s) + reference_value) * d);
    }

    return err;
}

int grib_accessor_data_simple_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    int err  = 0;
    size_t i = 0;
    for (i = 0; i < len; ++i) {
        if ((err = unpack_double_element(index_array[i], val_array + i)) != GRIB_SUCCESS)
            return err;
    }
    return GRIB_SUCCESS;
}

template <typename T>
int grib_accessor_data_simple_packing_t::unpack(T* val, size_t* len)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");

    grib_handle* gh         = grib_handle_of_accessor(this);
    unsigned char* buf      = (unsigned char*)grib_handle_of_accessor(this)->buffer->data;

    size_t i      = 0;
    int err       = 0;
    size_t n_vals = 0;
    long pos      = 0;
    long count    = 0;

    double reference_value;
    long binary_scale_factor;
    long bits_per_value;
    long decimal_scale_factor;
    long offsetBeforeData;
    double s            = 0;
    double d            = 0;
    double units_factor = 1.0;
    double units_bias   = 0.0;

    err = value_count(&count);
    if (err)
        return err;
    n_vals = count;

    if (*len < n_vals) {
        *len = (long)n_vals;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if ((err = grib_get_long_internal(gh, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;

    /*
     * check we don't decode bpv > max(ulong) as it is
     * not currently supported by the algorithm
     */
    if (bits_per_value > (sizeof(long) * 8)) {
        return GRIB_INVALID_BPV;
    }

    if (units_factor_ &&
        (grib_get_double_internal(gh, units_factor_, &units_factor) == GRIB_SUCCESS)) {
        grib_set_double_internal(gh, units_factor_, 1.0);
    }

    if (units_bias_ &&
        (grib_get_double_internal(gh, units_bias_, &units_bias) == GRIB_SUCCESS)) {
        grib_set_double_internal(gh, units_bias_, 0.0);
    }

    if (n_vals == 0) {
        *len = 0;
        return GRIB_SUCCESS;
    }

    dirty_ = 0;

    if ((err = grib_get_double_internal(gh, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    /* Special case */

    if (bits_per_value == 0) {
        for (i = 0; i < n_vals; i++)
            val[i] = reference_value;
        *len = n_vals;
        return GRIB_SUCCESS;
    }

    s = codes_power<T>(binary_scale_factor, 2);
    d = codes_power<T>(-decimal_scale_factor, 10);

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "%s %s: Creating %s, %zu values", class_name_, __func__, name_, n_vals);

    offsetBeforeData = byte_offset();
    buf += offsetBeforeData;

    /*ECCODES_ASSERT(((bits_per_value*n_vals)/8) < (1<<29));*/ /* See GRIB-787 */

    /* ECC-941 */
    if (!context_->ieee_packing) {
        /* Must turn off this check when the environment variable ECCODES_GRIB_IEEE_PACKING is on */
        long offsetAfterData = 0;
        err                  = grib_get_long(gh, "offsetAfterData", &offsetAfterData);
        if (!err && offsetAfterData > offsetBeforeData) {
            const long valuesSize = (bits_per_value * n_vals) / 8; /*in bytes*/
            if (offsetBeforeData + valuesSize > offsetAfterData) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "%s: Data section size mismatch: "
                                 "offset before data=%ld, offset after data=%ld (num values=%zu, bits per value=%ld)",
                                 class_name_, offsetBeforeData, offsetAfterData, n_vals, bits_per_value);
                return GRIB_DECODING_ERROR;
            }
        }
        //         if (offsetBeforeData == offsetAfterData) {
        //             /* Crazy case: Constant field with bitsPerValue > 0 */
        //             for (i = 0; i < n_vals; i++)
        //                 val[i] = reference_value;
        //             *len = n_vals;
        //             return GRIB_SUCCESS;
        //         }
    }

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "%s %s: calling outline function: bpv: %ld, rv: %g, bsf: %ld, dsf: %ld",
                     class_name_, __func__, bits_per_value, reference_value, binary_scale_factor, decimal_scale_factor);
    grib_decode_array<T>(buf, &pos, bits_per_value, reference_value, s, d, n_vals, val);

    *len = (long)n_vals;

    if (units_factor != 1.0) {
        if (units_bias != 0.0) {
            for (i = 0; i < n_vals; i++) {
                val[i] = val[i] * units_factor + units_bias;
            }
        }
        else {
            for (i = 0; i < n_vals; i++) {
                val[i] *= units_factor;
            }
        }
    }
    else if (units_bias != 0.0) {
        for (i = 0; i < n_vals; i++) {
            val[i] += units_bias;
        }
    }
    return err;
}

int grib_accessor_data_simple_packing_t::unpack_double(double* val, size_t* len)
{
    return unpack<double>(val, len);
}

int grib_accessor_data_simple_packing_t::unpack_float(float* val, size_t* len)
{
    return unpack<float>(val, len);
}

int grib_accessor_data_simple_packing_t::_unpack_double(double* val, size_t* len, unsigned char* buf, long pos, size_t n_vals)
{
    grib_accessor_data_simple_packing_t* self = (grib_accessor_data_simple_packing_t*)this;
    grib_handle* gh                           = grib_handle_of_accessor(this);

    size_t i = 0;
    int err  = 0;

    double reference_value;
    long binary_scale_factor;
    long bits_per_value;
    long decimal_scale_factor;
    long offsetBeforeData;
    double s            = 0;
    double d            = 0;
    double units_factor = 1.0;
    double units_bias   = 0.0;

    if (*len < n_vals) {
        *len = (long)n_vals;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if ((err = grib_get_long_internal(gh, self->bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;

    /*
     * check we don't decode bpv > max(ulong) as it is
     * not currently supported by the algorithm
     */
    if (bits_per_value > (sizeof(long) * 8)) {
        return GRIB_INVALID_BPV;
    }

    if (self->units_factor_ &&
        (grib_get_double_internal(gh, self->units_factor_, &units_factor) == GRIB_SUCCESS)) {
        grib_set_double_internal(gh, self->units_factor_, 1.0);
    }

    if (self->units_bias_ &&
        (grib_get_double_internal(gh, self->units_bias_, &units_bias) == GRIB_SUCCESS)) {
        grib_set_double_internal(gh, self->units_bias_, 0.0);
    }

    if (n_vals == 0) {
        *len = 0;
        return GRIB_SUCCESS;
    }

    dirty_ = 0;

    if ((err = grib_get_double_internal(gh, self->reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, self->binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, self->decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    /* Special case */

    if (bits_per_value == 0) {
        for (i = 0; i < n_vals; i++)
            val[i] = reference_value;
        *len = n_vals;
        return GRIB_SUCCESS;
    }

    s = codes_power<double>(binary_scale_factor, 2);
    d = codes_power<double>(-decimal_scale_factor, 10);

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "%s %s: Creating %s, %zu values", class_name_, __func__, name_, n_vals);

    offsetBeforeData = byte_offset();
    buf += offsetBeforeData;

    /*ECCODES_ASSERT(((bits_per_value*n_vals)/8) < (1<<29));*/ /* See GRIB-787 */

    /* ECC-941 */
    if (!context_->ieee_packing) {
        /* Must turn off this check when the environment variable ECCODES_GRIB_IEEE_PACKING is on */
        long offsetAfterData = 0;
        err                  = grib_get_long(gh, "offsetAfterData", &offsetAfterData);
        if (!err && offsetAfterData > offsetBeforeData) {
            const long valuesSize = (bits_per_value * n_vals) / 8; /*in bytes*/
            if (offsetBeforeData + valuesSize > offsetAfterData) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "Data section size mismatch: offset before data=%ld, offset after data=%ld (num values=%ld, bits per value=%ld)",
                                 offsetBeforeData, offsetAfterData, n_vals, bits_per_value);
                return GRIB_DECODING_ERROR;
            }
        }

        //         if (offsetBeforeData == offsetAfterData) {
        //             /* Crazy case: Constant field with bitsPerValue > 0 */
        //             for (i = 0; i < n_vals; i++)
        //                 val[i] = reference_value;
        //             *len = n_vals;
        //             return GRIB_SUCCESS;
        //         }
    }

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "unpack_double: calling outline function : bpv %d, rv : %g, sf : %d, dsf : %d ",
                     bits_per_value, reference_value, binary_scale_factor, decimal_scale_factor);
    grib_decode_array<double>(buf, &pos, bits_per_value, reference_value, s, d, n_vals, val);

    *len = (long)n_vals;

    if (units_factor != 1.0) {
        if (units_bias != 0.0)
            for (i = 0; i < n_vals; i++)
                val[i] = val[i] * units_factor + units_bias;
        else
            for (i = 0; i < n_vals; i++)
                val[i] *= units_factor;
    }
    else if (units_bias != 0.0)
        for (i = 0; i < n_vals; i++)
            val[i] += units_bias;

    return err;
}

int grib_accessor_data_simple_packing_t::unpack_double_subarray(double* val, size_t start, size_t len)
{
    unsigned char* buf  = (unsigned char*)grib_handle_of_accessor(this)->buffer->data;
    size_t nvals        = len;
    size_t* plen        = &len;
    long bits_per_value = 0;
    long pos            = 0;
    int err             = GRIB_SUCCESS;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), bits_per_value_, &bits_per_value)) !=
        GRIB_SUCCESS)
        return err;

    buf += (start * bits_per_value) / 8;
    pos = start * bits_per_value % 8;
    return _unpack_double(val, plen, buf, pos, nvals);
}

int grib_accessor_data_simple_packing_t::pack_double(const double* val, size_t* len)
{
    grib_handle* gh = grib_handle_of_accessor(this);

    size_t i                      = 0;
    size_t n_vals                 = *len;
    int err                       = 0;
    double reference_value        = 0;
    long binary_scale_factor      = 0;
    long bits_per_value           = 0;
    long decimal_scale_factor     = 0;
    long decimal_scale_factor_get = 0;
    long optimize_scaling_factor  = 0;
    double decimal                = 1;
    double max                    = 0;
    double min                    = 0;
    double unscaled_max           = 0;
    double unscaled_min           = 0;
    double f                      = 0;
    double range                  = 0;
    double minrange = 0, maxrange = 0;
    long changing_precision = 0;
    grib_context* c         = context_;

    decimal_scale_factor = 0;

    if (*len == 0) {
        return GRIB_NO_VALUES;
    }

    if ((err = grib_get_long_internal(gh, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;

    if (*len == 0)
        return GRIB_SUCCESS;

    if ((err = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor_get)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(gh, optimize_scaling_factor_, &optimize_scaling_factor)) != GRIB_SUCCESS)
        return err;

    /* check we don't encode bpv > max(ulong)-1 as it is not currently supported by the algorithm */
    if (bits_per_value > (sizeof(long) * 8 - 1)) {
        return GRIB_INVALID_BPV;
    }

    dirty_ = 1;

    max = val[0];
    min = max;
    for (i = 1; i < n_vals; i++) {
        if (val[i] > max)
            max = val[i];
        else if (val[i] < min)
            min = val[i];
    }

    if ((err = grib_check_data_values_minmax(gh, min, max)) != GRIB_SUCCESS) {
        return err;
    }

    /* constant field only reference_value is set and bits_per_value=0 */
    if (max == min) {
        int large_constant_fields = 0;
        if (grib_get_nearest_smaller_value(gh, reference_value_, val[0], &reference_value) != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR, "Unable to find nearest_smaller_value of %g for %s", min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }
        if ((err = grib_set_double_internal(gh, reference_value_, reference_value)) != GRIB_SUCCESS)
            return err;

        {
            /* Make sure we can decode it again */
            double ref = 1e-100;
            grib_get_double_internal(gh, reference_value_, &ref);
            if (ref != reference_value) {
                grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: %s (ref=%.10e != reference_value=%.10e)",
                                 class_name_, __func__, reference_value_, ref, reference_value);
                return GRIB_INTERNAL_ERROR;
            }
        }

        large_constant_fields = grib_producing_large_constant_fields(gh, edition_);
        if (large_constant_fields) {
            if ((err = grib_set_long_internal(gh, binary_scale_factor_, 0)) != GRIB_SUCCESS)
                return err;

            if ((err = grib_set_long_internal(gh, decimal_scale_factor_, 0)) != GRIB_SUCCESS)
                return err;

            if (bits_per_value == 0) {
                if ((err = grib_set_long_internal(gh, bits_per_value_, 16)) != GRIB_SUCCESS)
                    return err;
            }

            return GRIB_SUCCESS;
        }
        else {
            // ECC-2012
            if ((err = grib_set_long_internal(gh, binary_scale_factor_, 0)) != GRIB_SUCCESS)
                return err;

            bits_per_value = 0;
            if ((err = grib_set_long_internal(gh, bits_per_value_, bits_per_value)) != GRIB_SUCCESS)
                return err;

            return GRIB_CONSTANT_FIELD;
        }
    }

    if ((err = grib_get_long_internal(gh, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(gh, changing_precision_, &changing_precision)) != GRIB_SUCCESS)
        return err;

    /* the packing parameters are not properly defined, this is a safe way of fixing the problem */
    if (changing_precision == 0 && bits_per_value == 0 && decimal_scale_factor_get == 0) {
        grib_context_log(context_, GRIB_LOG_WARNING,
                         "%s==0 and %s==0 (setting %s=24)",
                         bits_per_value_,
                         decimal_scale_factor_,
                         bits_per_value_);

        bits_per_value = 24;
        if ((err = grib_set_long_internal(gh, bits_per_value_, bits_per_value)) != GRIB_SUCCESS)
            return err;
    }

    if (bits_per_value == 0 || (binary_scale_factor == 0 && decimal_scale_factor_get != 0)) {
        /* decimal_scale_factor is given, binary_scale_factor=0 and bits_per_value is computed */
        binary_scale_factor  = 0;
        decimal_scale_factor = decimal_scale_factor_get;
        decimal              = codes_power<double>(decimal_scale_factor, 10);
        min *= decimal;
        max *= decimal;

        /* bits_per_value=(long)ceil(log((double)(imax-imin+1))/log(2.0)); */
        /* See GRIB-540 for why we use ceil */
        err = number_of_bits((unsigned long)ceil(fabs(max - min)), &bits_per_value);
        if (err) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s %s: Range of values too large. Try a smaller value for decimal precision (less than %ld)",
                             class_name_, __func__, decimal_scale_factor);
            return err;
        }

        if ((err = grib_set_long_internal(gh, bits_per_value_, bits_per_value)) != GRIB_SUCCESS)
            return err;
        if (grib_get_nearest_smaller_value(gh, reference_value_, min, &reference_value) != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "Unable to find nearest_smaller_value of %g for %s", min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }
        /* divisor=1; */
    }
    else {
        int last = 127; /* 'last' should be a parameter coming from a definitions file */
        if (c->gribex_mode_on && edition_ == 1)
            last = 99;
        /* bits_per_value is given and decimal_scale_factor and binary_scale_factor are calcualated */
        if (max == min) {
            binary_scale_factor = 0;
            /* divisor=1; */
            if (grib_get_nearest_smaller_value(gh, reference_value_, min, &reference_value) != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "Unable to find nearest_smaller_value of %g for %s", min, reference_value_);
                return GRIB_INTERNAL_ERROR;
            }
        }
        else if (optimize_scaling_factor) {
            int compat_gribex = c->gribex_mode_on && edition_ == 1;
            if ((err = grib_optimize_decimal_factor(this, reference_value_,
                                                    max, min, bits_per_value,
                                                    compat_gribex, 1,
                                                    &decimal_scale_factor, &binary_scale_factor, &reference_value)) != GRIB_SUCCESS)
                return err;
        }
        else {
            /* printf("max=%g reference_value=%g codes_power<double>(-last,2)=%g decimal_scale_factor=%ld bits_per_value=%ld\n",
               max,reference_value,codes_power<double>(-last,2),decimal_scale_factor,bits_per_value);*/
            range        = (max - min);
            unscaled_min = min;
            unscaled_max = max;
            f            = (codes_power<double>(bits_per_value, 2) - 1);
            minrange     = codes_power<double>(-last, 2) * f;
            maxrange     = codes_power<double>(last, 2) * f;

            while (range < minrange) {
                decimal_scale_factor += 1;
                decimal *= 10;
                min   = unscaled_min * decimal;
                max   = unscaled_max * decimal;
                range = (max - min);
            }
            while (range > maxrange) {
                decimal_scale_factor -= 1;
                decimal /= 10;
                min   = unscaled_min * decimal;
                max   = unscaled_max * decimal;
                range = (max - min);
            }

            if (grib_get_nearest_smaller_value(gh, reference_value_,
                                               min, &reference_value) != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "Unable to find nearest_smaller_value of %g for %s", min, reference_value_);
                return GRIB_INTERNAL_ERROR;
            }

            binary_scale_factor = grib_get_binary_scale_fact(max, reference_value, bits_per_value, &err);
            if (err) return err;
        }
    }

    if ((err = grib_set_double_internal(gh, reference_value_, reference_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, changing_precision_, 0)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, binary_scale_factor_, binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(gh, decimal_scale_factor_, decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    return GRIB_SUCCESS;
}
