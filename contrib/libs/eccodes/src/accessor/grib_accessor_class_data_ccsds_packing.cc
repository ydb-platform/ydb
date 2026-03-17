/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_ccsds_packing.h"

#if defined(HAVE_LIBAEC) || defined(HAVE_AEC)
    #include <libaec.h>
    #ifndef LIBAEC_DLL_EXPORTED
        #error Version of libaec appears to be too old. Please upgrade.
    #endif
#endif

grib_accessor_data_ccsds_packing_t _grib_accessor_data_ccsds_packing{};
grib_accessor* grib_accessor_data_ccsds_packing = &_grib_accessor_data_ccsds_packing;

void grib_accessor_data_ccsds_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_values_t::init(v, args);

    grib_handle* h           = grib_handle_of_accessor(this);
    number_of_values_        = args->get_name(h, carg_++);
    reference_value_         = args->get_name(h, carg_++);
    binary_scale_factor_     = args->get_name(h, carg_++);
    decimal_scale_factor_    = args->get_name(h, carg_++);
    optimize_scaling_factor_ = args->get_name(h, carg_++);
    bits_per_value_          = args->get_name(h, carg_++);
    number_of_data_points_   = args->get_name(h, carg_++);
    ccsds_flags_             = args->get_name(h, carg_++);
    ccsds_block_size_        = args->get_name(h, carg_++);
    ccsds_rsi_               = args->get_name(h, carg_++);

    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_ccsds_packing_t::value_count(long* count)
{
    *count = 0;
    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, count);
}

#if defined(HAVE_LIBAEC) || defined(HAVE_AEC)

static bool is_big_endian()
{
    unsigned char is_big_endian   = 0;
    unsigned short endianess_test = 1;
    return reinterpret_cast<const char*>(&endianess_test)[0] == is_big_endian;
}

static void modify_aec_flags(long* flags)
{
    // ECC-1602: Performance improvement: enabled the use of native data types
    *flags &= ~AEC_DATA_3BYTE;  // disable support for 3-bytes per value
    if (is_big_endian())
        *flags |= AEC_DATA_MSB;  // enable big-endian
    else
        *flags &= ~AEC_DATA_MSB;  // enable little-endian
}

static const char* aec_get_error_message(int code)
{
    if (code == AEC_MEM_ERROR) return "AEC_MEM_ERROR";
    if (code == AEC_DATA_ERROR) return "AEC_DATA_ERROR";
    if (code == AEC_STREAM_ERROR) return "AEC_STREAM_ERROR";
    if (code == AEC_CONF_ERROR) return "AEC_CONF_ERROR";
    if (code == AEC_OK) return "AEC_OK";
    return "Unknown error code";
}

static void print_aec_stream_info(struct aec_stream* strm, const char* func)
{
    fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.flags=%u\n", func, strm->flags);
    fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.bits_per_sample=%u\n", func, strm->bits_per_sample);
    fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.block_size=%u\n", func, strm->block_size);
    fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.rsi=%u\n", func, strm->rsi);
    fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.avail_out=%lu\n", func, strm->avail_out);
    fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.avail_in=%lu\n", func, strm->avail_in);
}

#define MAX_BITS_PER_VALUE 32
int grib_accessor_data_ccsds_packing_t::pack_double(const double* val, size_t* len)
{
    grib_handle* hand       = grib_handle_of_accessor(this);
    int err                 = GRIB_SUCCESS;
    size_t buflen = 0, i = 0;
    bool is_constant_field = false;

    unsigned char* buf     = NULL;
    unsigned char* encoded = NULL;
    size_t n_vals          = 0;

    long binary_scale_factor  = 0;
    long decimal_scale_factor = 0;
    // long optimize_scaling_factor  = 0;
    double reference_value = 0;
    long bits_per_value    = 0;
    double max, min, d, divisor;

    long number_of_data_points;

    long ccsds_flags;
    long ccsds_block_size;
    long ccsds_rsi;

    struct aec_stream strm;

    dirty_ = 1;

    n_vals = *len;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    // if ((err = grib_get_long_internal(gh, optimize_scaling_factor_ , &optimize_scaling_factor)) != GRIB_SUCCESS)
    //     return err;

    if ((err = grib_get_long_internal(hand, ccsds_flags_, &ccsds_flags)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, ccsds_block_size_, &ccsds_block_size)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, ccsds_rsi_, &ccsds_rsi)) != GRIB_SUCCESS)
        return err;

    modify_aec_flags(&ccsds_flags);

    // Special case
    if (*len == 0) {
        grib_buffer_replace(this, NULL, 0, 1, 1);
        return GRIB_SUCCESS;
    }

    max = val[0];
    min = max;
    for (i = 1; i < n_vals; i++) {
        if (val[i] > max)
            max = val[i];
        else if (val[i] < min)
            min = val[i];
    }

    if ((err = grib_check_data_values_minmax(hand, min, max)) != GRIB_SUCCESS) {
        return err;
    }

    if (min == max) {
        is_constant_field = true;
    }
    else {
        if (bits_per_value == 0) {
            // ECC-1202: A non-constant field with bitsPerValue==0!
            bits_per_value = 24;  // Set sane value
        }
    }

    if (is_constant_field) {
    #ifdef DEBUG
        for (i = 1; i < n_vals; i++) {
            ECCODES_ASSERT(val[i] == val[0]);
        }
    #endif
        if (grib_get_nearest_smaller_value(hand, reference_value_, val[0], &reference_value) != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s %s: Unable to find nearest_smaller_value of %g for %s", class_name_, __func__, min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }
        if ((err = grib_set_double_internal(hand, reference_value_, reference_value)) != GRIB_SUCCESS)
            return err;

        if ((err = grib_set_long_internal(hand, number_of_values_, n_vals)) != GRIB_SUCCESS)
            return err;

        // ECC-2012
        if ((err = grib_set_long_internal(hand, binary_scale_factor_, 0)) != GRIB_SUCCESS)
            return err;

        bits_per_value = 0;  // ECC-1387
        if ((err = grib_set_long_internal(hand, bits_per_value_, bits_per_value)) != GRIB_SUCCESS)
            return err;

        grib_buffer_replace(this, NULL, 0, 1, 1);

        return GRIB_SUCCESS;
    }

    if ((err = grib_get_long_internal(hand, number_of_data_points_, &number_of_data_points)) != GRIB_SUCCESS)
        return err;

    if (bits_per_value == 0 || (binary_scale_factor == 0 && decimal_scale_factor != 0)) {
        d = codes_power<double>(decimal_scale_factor, 10);
        min *= d;
        max *= d;

        if (grib_get_nearest_smaller_value(hand, reference_value_, min, &reference_value) != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s %s: Unable to find nearest_smaller_value of %g for %s", class_name_, __func__, min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }

        if (reference_value > min) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s %s: reference_value=%g min_value=%g diff=%g", class_name_, __func__, reference_value, min, reference_value - min);
            DEBUG_ASSERT(reference_value <= min);
            return GRIB_INTERNAL_ERROR;
        }
    }
    else {
        int last        = 127;  // last must be a parameter coming from the def file
        double range    = 0;
        double minrange = 0, maxrange = 0;
        double unscaled_max = 0;
        double unscaled_min = 0;
        double f            = 0;
        double decimal      = 1;

        decimal_scale_factor = 0;
        range                = (max - min);
        unscaled_min         = min;
        unscaled_max         = max;
        f                    = (codes_power<double>(bits_per_value, 2) - 1);
        minrange             = codes_power<double>(-last, 2) * f;
        maxrange             = codes_power<double>(last, 2) * f;

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

        if (grib_get_nearest_smaller_value(hand, reference_value_, min, &reference_value) != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s %s: Unable to find nearest_smaller_value of %g for %s", class_name_, __func__, min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }
        d = codes_power<double>(decimal_scale_factor, 10);
    }

    binary_scale_factor = grib_get_binary_scale_fact(max, reference_value, bits_per_value, &err);
    if (err) return err;
    divisor = codes_power<double>(-binary_scale_factor, 2);

    size_t nbytes = (bits_per_value + 7) / 8;
    // ECC-1602: use native a data type (4 bytes for uint32_t) for values that require only 3 bytes
    if (nbytes == 3)
        nbytes = 4;

    encoded = reinterpret_cast<unsigned char*>(grib_context_buffer_malloc_clear(context_, nbytes * n_vals));

    if (!encoded) {
        err = GRIB_OUT_OF_MEMORY;
        goto cleanup;
    }

    /*
    // Original code is memory efficient and supports 3 bytes per value
    // replaced by ECC-1602 for performance reasons
    buflen = 0;
    p      = encoded;
    for (i = 0; i < n_vals; i++) {
        long blen                  = bits8;
        unsigned long unsigned_val = (unsigned long)((((val[i] * d) - reference_value) * divisor) + 0.5);
        while (blen >= 8) {
            blen -= 8;
            *p = (unsigned_val >> blen);
            p++;
            buflen++;
        }
    }
    */

    // ECC-1602: Performance improvement
    switch (nbytes) {
        case 1:
            for (i = 0; i < n_vals; i++) {
                encoded[i] = static_cast<uint8_t>(((val[i] * d - reference_value) * divisor) + 0.5);
            }
            break;
        case 2:
            for (i = 0; i < n_vals; i++) {
                reinterpret_cast<uint16_t*>(encoded)[i] = static_cast<uint16_t>(((val[i] * d - reference_value) * divisor) + 0.5);
            }
            break;
        case 4:
            for (i = 0; i < n_vals; i++) {
                reinterpret_cast<uint32_t*>(encoded)[i] = static_cast<uint32_t>(((val[i] * d - reference_value) * divisor) + 0.5);
            }
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "%s pack_double: packing %s, bitsPerValue=%ld (max %ld)",
                             class_name_, name_, bits_per_value, MAX_BITS_PER_VALUE);
            err = GRIB_INVALID_BPV;
            goto cleanup;
    }

    grib_context_log(context_, GRIB_LOG_DEBUG, "%s pack_double: packing %s, %zu values", class_name_, name_, n_vals);

    // ECC-1431: GRIB2: CCSDS encoding failure AEC_STREAM_ERROR
    buflen = (nbytes * n_vals) * 67 / 64 + 256;
    buf    = (unsigned char*)grib_context_buffer_malloc_clear(context_, buflen);

    if (!buf) {
        err = GRIB_OUT_OF_MEMORY;
        goto cleanup;
    }

    if ((err = grib_set_double_internal(hand, reference_value_, reference_value)) != GRIB_SUCCESS)
        return err;
    {
        // Make sure we can decode it again
        double ref = 1e-100;
        grib_get_double_internal(hand, reference_value_, &ref);
        if (ref != reference_value) {
            grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: %s (ref=%.10e != reference_value=%.10e)",
                             class_name_, __func__, reference_value_, ref, reference_value);
            return GRIB_INTERNAL_ERROR;
        }
    }

    if ((err = grib_set_long_internal(hand, binary_scale_factor_, binary_scale_factor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_set_long_internal(hand, decimal_scale_factor_, decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    strm.flags           = ccsds_flags;
    strm.bits_per_sample = bits_per_value;
    strm.block_size      = ccsds_block_size;
    strm.rsi             = ccsds_rsi;

    strm.next_out  = buf;
    strm.avail_out = buflen;
    strm.next_in   = encoded;
    strm.avail_in  = nbytes * n_vals;

    // This does not support spherical harmonics, and treats 24 differently than:
    // see http://cdo.sourcearchive.com/documentation/1.5.1.dfsg.1-1/cgribexlib_8c_source.html

    if (hand->context->debug) print_aec_stream_info(&strm, "pack_double");

    if ((err = aec_buffer_encode(&strm)) != AEC_OK) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: aec_buffer_encode error %d (%s)",
                         class_name_, __func__, err, aec_get_error_message(err));
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }

    buflen = strm.total_out;
    grib_buffer_replace(this, buf, buflen, 1, 1);

cleanup:
    grib_context_buffer_free(context_, buf);
    grib_context_buffer_free(context_, encoded);

    if (err == GRIB_SUCCESS)
        err = grib_set_long_internal(hand, number_of_values_, *len);

    if (err == GRIB_SUCCESS)
        err = grib_set_long_internal(hand, bits_per_value_, strm.bits_per_sample);

    return err;
}

template <typename T>
int grib_accessor_data_ccsds_packing_t::unpack(T* val, size_t* len)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");
    grib_handle* hand       = grib_handle_of_accessor(this);

    int err = GRIB_SUCCESS, i = 0;
    size_t buflen = 0;
    struct aec_stream strm;
    double bscale          = 0;
    double dscale          = 0;
    unsigned char* buf     = NULL;
    size_t n_vals          = 0;
    size_t size            = 0;
    unsigned char* decoded = NULL;
    // unsigned char* p       = NULL;
    long nn = 0;

    long binary_scale_factor  = 0;
    long decimal_scale_factor = 0;
    double reference_value    = 0;
    long bits_per_value       = 0;

    long ccsds_flags;
    long ccsds_block_size;
    long ccsds_rsi;
    size_t nbytes;

    dirty_ = 0;

    if ((err = value_count(&nn)) != GRIB_SUCCESS)
        return err;
    n_vals = nn;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    // ECC-477: Don't call grib_get_long_internal to suppress error message being output
    if ((err = grib_get_long(hand, ccsds_flags_, &ccsds_flags)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(hand, ccsds_block_size_, &ccsds_block_size)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, ccsds_rsi_, &ccsds_rsi)) != GRIB_SUCCESS)
        return err;

    modify_aec_flags(&ccsds_flags);

    // TODO(masn): This should be called upstream
    if (*len < n_vals)
        return GRIB_ARRAY_TOO_SMALL;

    // Special case
    if (bits_per_value == 0) {
        for (i = 0; i < n_vals; i++)
            val[i] = reference_value;
        *len = n_vals;
        return GRIB_SUCCESS;
    }

    bscale = codes_power<T>(binary_scale_factor, 2);
    dscale = codes_power<T>(-decimal_scale_factor, 10);

    buflen = byte_count();
    buf    = (unsigned char*)hand->buffer->data;
    buf += byte_offset();
    strm.flags           = ccsds_flags;
    strm.bits_per_sample = bits_per_value;
    strm.block_size      = ccsds_block_size;
    strm.rsi             = ccsds_rsi;

    strm.next_in  = buf;
    strm.avail_in = buflen;

    nbytes = (bits_per_value + 7) / 8;
    if (nbytes == 3)
        nbytes = 4;

    size    = n_vals * nbytes;
    decoded = (unsigned char*)grib_context_buffer_malloc_clear(context_, size);
    if (!decoded) {
        err = GRIB_OUT_OF_MEMORY;
        goto cleanup;
    }
    strm.next_out  = decoded;
    strm.avail_out = size;

    if (hand->context->debug) print_aec_stream_info(&strm, "unpack_*");

    if ((err = aec_buffer_decode(&strm)) != AEC_OK) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: aec_buffer_decode error %d (%s)",
                         class_name_, __func__, err, aec_get_error_message(err));
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    // ECC-1427: Performance improvement (replaced by ECC-1602)
    // grib_decode_array<T>(decoded, &pos, bits8 , reference_value, bscale, dscale, n_vals, val);

    // ECC-1602: Performance improvement
    switch (nbytes) {
        case 1:
            for (i = 0; i < n_vals; i++) {
                val[i] = (reinterpret_cast<uint8_t*>(decoded)[i] * bscale + reference_value) * dscale;
            }
            break;
        case 2:
            for (i = 0; i < n_vals; i++) {
                val[i] = (reinterpret_cast<uint16_t*>(decoded)[i] * bscale + reference_value) * dscale;
            }
            break;
        case 4:
            for (i = 0; i < n_vals; i++) {
                val[i] = (reinterpret_cast<uint32_t*>(decoded)[i] * bscale + reference_value) * dscale;
            }
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: unpacking %s, bitsPerValue=%ld (max %ld)",
                             class_name_, __func__, name_, bits_per_value, MAX_BITS_PER_VALUE);
            err = GRIB_INVALID_BPV;
            goto cleanup;
    }

    *len = n_vals;

cleanup:
    grib_context_buffer_free(context_, decoded);
    return err;
}

int grib_accessor_data_ccsds_packing_t::unpack_double(double* val, size_t* len)
{
    return unpack<double>(val, len);
}

int grib_accessor_data_ccsds_packing_t::unpack_float(float* val, size_t* len)
{
    return unpack<float>(val, len);
}

int grib_accessor_data_ccsds_packing_t::unpack_double_element(size_t idx, double* val)
{
    // The index idx relates to codedValues NOT values!
    grib_handle* hand      = grib_handle_of_accessor(this);
    int err                = 0;
    size_t size            = 0;
    long bits_per_value    = 0;
    double reference_value = 0;
    double* values         = NULL;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    // Special case of constant field
    if (bits_per_value == 0) {
        *val = reference_value;
        return GRIB_SUCCESS;
    }

    err = grib_get_size(hand, "codedValues", &size);
    if (err) return err;
    if (idx > size) return GRIB_INVALID_ARGUMENT;

    values = (double*)grib_context_malloc_clear(context_, size * sizeof(double));
    err    = grib_get_double_array(hand, "codedValues", values, &size);
    if (err) {
        grib_context_free(context_, values);
        return err;
    }
    *val = values[idx];
    grib_context_free(context_, values);
    return GRIB_SUCCESS;
}

int grib_accessor_data_ccsds_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    size_t size = 0, i = 0;
    double* values         = NULL;
    int err                = 0;
    long bits_per_value    = 0;
    double reference_value = 0;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    // Special case of constant field
    if (bits_per_value == 0) {
        for (i = 0; i < len; i++) {
            val_array[i] = reference_value;
        }
        return GRIB_SUCCESS;
    }

    // GRIB-564: The indexes in index_array relate to codedValues NOT values!
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

#else

static void print_error_feature_not_enabled(grib_context* c)
{
    grib_context_log(c, GRIB_LOG_ERROR,
                     "CCSDS support not enabled. "
                     "Please rebuild with -DENABLE_AEC=ON (Adaptive Entropy Coding library)");
}
int grib_accessor_data_ccsds_packing_t::pack_double(const double* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_ccsds_packing_t::unpack_double(double* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_ccsds_packing_t::unpack_float(float* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_ccsds_packing_t::unpack_double_element(size_t idx, double* val)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_ccsds_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}

#endif
