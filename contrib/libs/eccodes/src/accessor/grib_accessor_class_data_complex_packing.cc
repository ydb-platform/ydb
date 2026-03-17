/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_complex_packing.h"

grib_accessor_data_complex_packing_t _grib_accessor_data_complex_packing{};
grib_accessor* grib_accessor_data_complex_packing = &_grib_accessor_data_complex_packing;

void grib_accessor_data_complex_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);
    grib_handle* gh = grib_handle_of_accessor(this);

    GRIBEX_sh_bug_present_  = args->get_name(gh, carg_++);
    ieee_floats_            = args->get_name(gh, carg_++);
    laplacianOperatorIsSet_ = args->get_name(gh, carg_++);
    laplacianOperator_      = args->get_name(gh, carg_++);
    sub_j_                  = args->get_name(gh, carg_++);
    sub_k_                  = args->get_name(gh, carg_++);
    sub_m_                  = args->get_name(gh, carg_++);
    pen_j_                  = args->get_name(gh, carg_++);
    pen_k_                  = args->get_name(gh, carg_++);
    pen_m_                  = args->get_name(gh, carg_++);

    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_complex_packing_t::value_count(long* count)
{
    int ret         = GRIB_SUCCESS;
    grib_handle* gh = grib_handle_of_accessor(this);
    long pen_j      = 0;
    long pen_k      = 0;
    long pen_m      = 0;
    *count          = 0;

    if (length_ == 0)
        return 0;

    if ((ret = grib_get_long_internal(gh, pen_j_, &pen_j)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_k_, &pen_k)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_m_, &pen_m)) != GRIB_SUCCESS)
        return ret;

    if (pen_j != pen_k || pen_j != pen_m) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Invalid pentagonal resolution parameters");
        grib_context_log(context_, GRIB_LOG_ERROR, "pen_j=%ld, pen_k=%ld, pen_m=%ld", pen_j, pen_k, pen_m);
        return GRIB_DECODING_ERROR;
    }
    *count = (pen_j + 1) * (pen_j + 2);

    return ret;
}

double calculate_pfactor(const grib_context* ctx, const double* spectralField, long fieldTruncation, long subsetTruncation)
{
    /*long n_vals = ((fieldTruncation+1)*(fieldTruncation+2));*/
    long loop, index, m, n = 0;
    double pFactor, zeps = 1.0e-15;
    long ismin = (subsetTruncation + 1), ismax = (fieldTruncation + 1);
    double *weights, range, *norms;
    double weightedSumOverX = 0.0, weightedSumOverY = 0.0, sumOfWeights = 0.0, x, y;
    double numerator = 0.0, denominator = 0.0, slope;

    /* Catch corner case. See GRIB-172 */
    if (ismax - ismin <= 1) {
        return 1; /* any value will do! we cannot do linear fit on a single point! */
    }

    /*
     * Setup the weights
     */
    range   = (double)(ismax - ismin + 1);
    weights = (double*)grib_context_malloc(ctx, (ismax + 1) * sizeof(double));
    for (loop = ismin; loop <= ismax; loop++)
        weights[loop] = range / (double)(loop - ismin + 1);

    /*
     * Compute norms
     * Handle values 2 at a time (real and imaginary parts).
     */
    norms = (double*)grib_context_malloc(ctx, (ismax + 1) * sizeof(double));
    for (loop = 0; loop < ismax + 1; loop++)
        norms[loop] = 0.0;

    /*
     * Form norms for the rows which contain part of the unscaled subset.
     */
    index = -2;
    for (m = 0; m < subsetTruncation; m++) {
        for (n = m; n <= fieldTruncation; n++) {
            index += 2;
            if (n >= subsetTruncation) {
                norms[n] = std::max(norms[n], fabs(spectralField[index]));
                norms[n] = std::max(norms[n], fabs(spectralField[index + 1]));
            }
        }
    }

    /*
     * Form norms for the rows which do not contain part of the unscaled subset.
     */
    for (m = subsetTruncation; m <= fieldTruncation; m++) {
        for (n = m; n <= fieldTruncation; n++) {
            index += 2;
            norms[n] = std::max(norms[n], fabs(spectralField[index]));
            norms[n] = std::max(norms[n], fabs(spectralField[index + 1]));
        }
    }

    /*
     * Ensure the norms have a value which is not too small in case of
     * problems with math functions (e.g. LOG).
     */
    for (loop = ismin; loop <= ismax; loop++) {
        norms[loop] = std::max(norms[loop], zeps);
        if (norms[loop] == zeps)
            weights[loop] = 100.0 * zeps;
    }

    /*
     * Do linear fit to find the slope
     */
    for (loop = ismin; loop <= ismax; loop++) {
        x = log((double)(loop * (loop + 1)));
        ECCODES_ASSERT(norms[loop] > 0);
        y                = log(norms[loop]);
        weightedSumOverX = weightedSumOverX + x * weights[loop];
        weightedSumOverY = weightedSumOverY + y * weights[loop];
        sumOfWeights     = sumOfWeights + weights[loop];
    }
    weightedSumOverX = weightedSumOverX / sumOfWeights;
    weightedSumOverY = weightedSumOverY / sumOfWeights;

    /*
     * Perform a least square fit for the equation
     */
    for (loop = ismin; loop <= ismax; loop++) {
        x = log((double)(loop * (loop + 1)));
        y = log(norms[loop]);
        numerator =
            numerator + weights[loop] * (y - weightedSumOverY) * (x - weightedSumOverX);
        denominator =
            denominator + weights[loop] * ((x - weightedSumOverX) * (x - weightedSumOverX));
    }
    slope = numerator / denominator;

    grib_context_free(ctx, weights);
    grib_context_free(ctx, norms);

    pFactor = -slope;
    if (pFactor < -9999.9)
        pFactor = -9999.9;
    if (pFactor > 9999.9)
        pFactor = 9999.9;
    return pFactor;
}

int grib_accessor_data_complex_packing_t::pack_double(const double* val, size_t* len)
{
    grib_handle* gh = grib_handle_of_accessor(this);

    size_t i      = 0;
    int ret       = GRIB_SUCCESS;
    long hcount   = 0;
    long lcount   = 0;
    long hpos     = 0;
    long lup      = 0;
    long mmax     = 0;
    long n_vals   = 0;
    double* scals = NULL;

    double s = 0;
    double d = 0;

    unsigned char* buf = NULL;

    size_t buflen = 0;
    size_t hsize  = 0;
    size_t lsize  = 0;

    unsigned char* hres = NULL;
    unsigned char* lres = NULL;

    long lpos = 0;
    long maxv = 0;

    long offsetdata              = 0;
    long bits_per_value          = 0;
    double reference_value       = 0;
    long binary_scale_factor     = 0;
    long decimal_scale_factor    = 0;
    long optimize_scaling_factor = 0;
    long laplacianOperatorIsSet  = 0;

    double laplacianOperator   = 0;
    long sub_j                 = 0;
    long sub_k                 = 0;
    long sub_m                 = 0;
    long pen_j                 = 0;
    long pen_k                 = 0;
    long pen_m                 = 0;
    long GRIBEX_sh_bug_present = 0;
    long ieee_floats           = 0;
    double min                 = 0;
    double max                 = 0;
    double current_val         = 0;
    short mixmax_unset         = 0;
    int bytes;

    encode_float_proc encode_float = NULL;

    if (*len == 0)
        return GRIB_NO_VALUES;

    if ((ret = grib_get_long_internal(gh, offsetdata_, &offsetdata)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, optimize_scaling_factor_, &optimize_scaling_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, GRIBEX_sh_bug_present_, &GRIBEX_sh_bug_present)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, ieee_floats_, &ieee_floats)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, laplacianOperatorIsSet_, &laplacianOperatorIsSet)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(gh, laplacianOperator_, &laplacianOperator)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, sub_j_, &sub_j)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, sub_k_, &sub_k)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, sub_m_, &sub_m)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_j_, &pen_j)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_k_, &pen_k)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_m_, &pen_m)) != GRIB_SUCCESS)
        return ret;

    dirty_ = 1;

    switch (ieee_floats) {
        case 0:
            encode_float = grib_ibm_to_long;
            bytes        = 4;
            break;
        case 1:
            encode_float = grib_ieee_to_long;
            bytes        = 4;
            break;
        case 2:
            encode_float = grib_ieee64_to_long;
            bytes        = 8;
            break;
        default:
            return GRIB_NOT_IMPLEMENTED;
    }

    if (sub_j != sub_k || sub_j != sub_m || pen_j != pen_k || pen_j != pen_m) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s: Invalid pentagonal resolution parameters", class_name_);
        return GRIB_ENCODING_ERROR;
    }

    n_vals = (pen_j + 1) * (pen_j + 2);

    if (*len != n_vals) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s: Wrong number of values, expected %ld - got %zu",
                         class_name_, n_vals, *len);
        return GRIB_INTERNAL_ERROR;
    }

    /* Data Quality checks */
    if (context_->grib_data_quality_checks) {
        /* First value is the field's average */
        double min_val = val[0];
        double max_val = min_val;
        if ((ret = grib_util_grib_data_quality_check(gh, min_val, max_val)) != GRIB_SUCCESS)
            return ret;
    }

    if (pen_j == sub_j) {
        double* values;
        d = codes_power<double>(decimal_scale_factor, 10);
        if (d) {
            values = (double*)grib_context_malloc_clear(context_, sizeof(double) * n_vals);
            for (i = 0; i < n_vals; i++)
                values[i] = val[i] * d;
        }
        else {
            values = (double*)val;
        }
        buflen = n_vals * bytes;
        buf    = (unsigned char*)grib_context_malloc_clear(context_, buflen);
        grib_ieee_encode_array(context_, values, n_vals, bytes, buf);
        if (d)
            grib_context_free(context_, values);
        grib_buffer_replace(this, buf, buflen, 1, 1);
        grib_context_free(context_, buf);
        return 0;
    }

    if (!laplacianOperatorIsSet) {
        laplacianOperator = calculate_pfactor(context_, val, pen_j, sub_j);
        if ((ret = grib_set_double_internal(gh, laplacianOperator_, laplacianOperator)) != GRIB_SUCCESS)
            return ret;
        grib_get_double_internal(gh, laplacianOperator_, &laplacianOperator);
    }

    hsize = bytes * (sub_k + 1) * (sub_k + 2);
    lsize = ((n_vals - ((sub_k + 1) * (sub_k + 2))) * bits_per_value) / 8;

    buflen = hsize + lsize;

    buf  = (unsigned char*)grib_context_malloc(context_, buflen);
    hres = buf;
    lres = buf + hsize;

    maxv = pen_j + 1;

    lpos = 0;
    hpos = 0;

    scals = (double*)grib_context_malloc(context_, maxv * sizeof(double));
    if (!scals) return GRIB_OUT_OF_MEMORY;

    scals[0] = 0;
    for (i = 1; i < maxv; i++)
        scals[i] = ((double)pow(i * (i + 1), laplacianOperator));

    mmax   = 0;
    maxv   = pen_j + 1;
    i      = 0;
    hcount = 0;
    sub_k  = sub_j;

    while (maxv > 0) {
        lup = mmax;

        if (sub_k >= 0) {
            i += 2 * (sub_k + 1);
            lup += sub_k + 1;
            hcount += sub_k + 1;
            sub_k--;
        }

        for (lcount = hcount; lcount < maxv; lcount++) {
            current_val = ((val[i++]) * scals[lup]);
            if (mixmax_unset == 0) {
                max          = current_val;
                min          = current_val;
                mixmax_unset = 1;
            }

            if (current_val > max)
                max = current_val;
            if (current_val < min)
                min = current_val;

            current_val = ((val[i++]) * scals[lup]);
            if (current_val > max)
                max = current_val;
            if (current_val < min)
                min = current_val;

            lup++;
        }
        maxv--;
        hcount = 0;
        mmax++;
    }

    if (optimize_scaling_factor) {
        ret = grib_optimize_decimal_factor(this, reference_value_,
                                           max, min, bits_per_value, 0, 1,
                                           &decimal_scale_factor,
                                           &binary_scale_factor,
                                           &reference_value);
        if (ret != GRIB_SUCCESS) {
            grib_context_log(gh->context, GRIB_LOG_ERROR,
                             "%s: Unable to find nearest_smaller_value of %g for %s", class_name_, min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }
        d = codes_power<double>(+decimal_scale_factor, 10);
    }
    else {
        d = codes_power<double>(+decimal_scale_factor, 10);
        if (grib_get_nearest_smaller_value(gh, reference_value_, d * min, &reference_value) != GRIB_SUCCESS) {
            grib_context_log(gh->context, GRIB_LOG_ERROR,
                             "%s: Unable to find nearest_smaller_value of %g for %s", class_name_, d * min, reference_value_);
            return GRIB_INTERNAL_ERROR;
        }
        binary_scale_factor = grib_get_binary_scale_fact(d * max, reference_value, bits_per_value, &ret);
        if (ret == GRIB_UNDERFLOW) {
            d                   = 0;
            binary_scale_factor = 0;
            reference_value     = 0;
        }
        else {
            if (ret != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "%s: Cannot compute binary_scale_factor", class_name_);
                return ret;
            }
        }
    }
    s = codes_power<double>(-binary_scale_factor, 2);

    i = 0;

    mmax   = 0;
    maxv   = pen_j + 1;
    hcount = 0;
    sub_k  = sub_j;

    while (maxv > 0) {
        lup = mmax;

        if (sub_k >= 0) {
            for (hcount = 0; hcount < sub_k + 1; hcount++) {
                if (GRIBEX_sh_bug_present && hcount == sub_k) {
                    /* _test(val[i]*d*scals[lup],1); */
                    grib_encode_unsigned_long(hres, encode_float((val[i++] * d) * scals[lup]), &hpos, 8 * bytes);
                    /* _test(val[i]*d*scals[lup],1); */
                    grib_encode_unsigned_long(hres, encode_float((val[i++] * d) * scals[lup]), &hpos, 8 * bytes);
                }
                else {
                    /* _test(val[i]*d,0); */
                    grib_encode_unsigned_long(hres, encode_float(val[i++]), &hpos, 8 * bytes);
                    /* _test(val[i]*d,0); */
                    grib_encode_unsigned_long(hres, encode_float(val[i++]), &hpos, 8 * bytes);
                }
                lup++;
            }
            sub_k--;
        }

#if FAST_BIG_ENDIAN
        grib_encode_double_array_complex((maxv - hcount) * 2, &(val[i]), bits_per_value, reference_value, &(scals[lup]), d, s, lres, &lpos);
        i += (maxv - hcount) * 2;
#else
        if (bits_per_value % 8) {
            for (lcount = hcount; lcount < maxv; lcount++) {
                current_val = (((((val[i++] * d) * scals[lup]) - reference_value) * s) + 0.5);
                if (current_val < 0)
                    grib_context_log(context_, GRIB_LOG_ERROR,
                                     "%s: negative coput before packing (%g)", class_name_, current_val);
                grib_encode_unsigned_longb(lres, current_val, &lpos, bits_per_value);

                current_val = (((((val[i++] * d) * scals[lup]) - reference_value) * s) + 0.5);
                if (current_val < 0)
                    grib_context_log(context_, GRIB_LOG_ERROR,
                                     "%s: negative coput before packing (%g)", class_name_, current_val);
                grib_encode_unsigned_longb(lres, current_val, &lpos, bits_per_value);
                lup++;
            }
        }
        else {
            for (lcount = hcount; lcount < maxv; lcount++) {
                current_val = (((((val[i++] * d) * scals[lup]) - reference_value) * s) + 0.5);
                if (current_val < 0)
                    grib_context_log(context_, GRIB_LOG_ERROR,
                                     "%s: negative coput before packing (%g)", class_name_, current_val);
                grib_encode_unsigned_long(lres, current_val, &lpos, bits_per_value);

                current_val = (((((val[i++] * d) * scals[lup]) - reference_value) * s) + 0.5);
                if (current_val < 0)
                    grib_context_log(context_, GRIB_LOG_ERROR,
                                     "%s: negative coput before packing (%g)", class_name_, current_val);
                grib_encode_unsigned_long(lres, current_val, &lpos, bits_per_value);
                lup++;
            }
        }
#endif

        maxv--;
        hcount = 0;
        mmax++;
    }

    if (((hpos / 8) != hsize) && ((lpos / 8) != lsize)) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Mismatch in packing between high resolution and low resolution part", class_name_);
        grib_context_free(context_, buf);
        grib_context_free(context_, scals);
        return GRIB_INTERNAL_ERROR;
    }

    buflen = ((hpos + lpos) / 8);

    if ((ret = grib_set_double_internal(gh, reference_value_, reference_value)) != GRIB_SUCCESS)
        return ret;
    {
        // Make sure we can decode it again
        double ref = 1e-100;
        grib_get_double_internal(gh, reference_value_, &ref);
        if (ref != reference_value) {
            grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: %s (ref=%.10e != reference_value=%.10e)",
                             class_name_, __func__, reference_value_, ref, reference_value);
            return GRIB_INTERNAL_ERROR;
        }
    }

    if ((ret = grib_set_long_internal(gh, binary_scale_factor_, binary_scale_factor)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(gh, decimal_scale_factor_, decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    grib_buffer_replace(this, buf, buflen, 1, 1);
    grib_context_free(context_, buf);
    grib_context_free(context_, scals);

    return ret;
}

template <typename T>
int grib_accessor_data_complex_packing_t::unpack_real(T* val, size_t* len)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");
    grib_handle* gh         = grib_handle_of_accessor(this);

    size_t i    = 0;
    int ret     = GRIB_SUCCESS;
    long hcount = 0;
    long lcount = 0;
    long hpos   = 0;
    long lup    = 0;
    long mmax   = 0;
    long n_vals = 0;
    T* scals    = NULL;
    T *pscals = NULL, *pval = NULL;

    T s                 = 0;
    T d                 = 0;
    T laplacianOperator = 0;
    unsigned char* buf  = NULL;
    unsigned char* hres = NULL;
    unsigned char* lres = NULL;
    unsigned long packed_offset;
    long lpos = 0;

    long maxv                  = 0;
    long GRIBEX_sh_bug_present = 0;
    long ieee_floats           = 0;

    long offsetdata           = 0;
    long bits_per_value       = 0;
    T reference_value         = 0;
    long binary_scale_factor  = 0;
    long decimal_scale_factor = 0;

    long sub_j = 0;
    long sub_k = 0;
    long sub_m = 0;
    long pen_j = 0;
    long pen_k = 0;
    long pen_m = 0;

    T operat = 0;
    int bytes;
    int err = 0;
    double tmp;

    decode_float_proc decode_float = NULL;

    err = value_count(&n_vals);
    if (err)
        return err;

    if (*len < n_vals) {
        *len = n_vals;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if ((ret = grib_get_long_internal(gh, offsetdata_, &offsetdata)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_double_internal(gh, reference_value_, &tmp)) != GRIB_SUCCESS)
        return ret;
    reference_value = tmp;
    if ((ret = grib_get_long_internal(gh, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(gh, GRIBEX_sh_bug_present_, &GRIBEX_sh_bug_present)) != GRIB_SUCCESS)
        return ret;

    /* ECC-774: don't use grib_get_long_internal */
    if ((ret = grib_get_long(gh, ieee_floats_, &ieee_floats)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(gh, laplacianOperator_, &tmp)) != GRIB_SUCCESS)
        return ret;
    laplacianOperator = tmp;

    if ((ret = grib_get_long_internal(gh, sub_j_, &sub_j)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, sub_k_, &sub_k)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, sub_m_, &sub_m)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_j_, &pen_j)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_k_, &pen_k)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(gh, pen_m_, &pen_m)) != GRIB_SUCCESS)
        return ret;

    dirty_ = 0;

    switch (ieee_floats) {
        case 0:
            decode_float = grib_long_to_ibm;
            bytes        = 4;
            break;
        case 1:
            decode_float = grib_long_to_ieee;
            bytes        = 4;
            break;
        case 2:
            decode_float = grib_long_to_ieee64;
            bytes        = 8;
            break;
        default:
            return GRIB_NOT_IMPLEMENTED;
    }

    if (sub_j != sub_k || sub_j != sub_m || pen_j != pen_k || pen_j != pen_m) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s: Invalid pentagonal resolution parameters", class_name_);
        return GRIB_DECODING_ERROR;
    }

    buf = (unsigned char*)gh->buffer->data;

    maxv = pen_j + 1;

    buf += byte_offset();
    hres = buf;
    lres = buf;

    if (pen_j == sub_j) {
        n_vals = (pen_j + 1) * (pen_j + 2);
        d      = codes_power<T>(-decimal_scale_factor, 10);

        grib_ieee_decode_array<T>(context_, buf, n_vals, bytes, val);
        if (d) {
            for (i = 0; i < n_vals; i++)
                val[i] *= d;
        }
        return 0;
    }

    packed_offset = byte_offset() + bytes * (sub_k + 1) * (sub_k + 2);
    lpos          = 8 * (packed_offset - offsetdata);

    s = codes_power<T>(binary_scale_factor, 2);
    d = codes_power<T>(-decimal_scale_factor, 10);

    scals = (T*)grib_context_malloc(context_, maxv * sizeof(T));
    if (!scals) return GRIB_OUT_OF_MEMORY;

    scals[0] = 0;
    for (i = 1; i < maxv; i++) {
        operat = pow(i * (i + 1), laplacianOperator);
        if (operat != 0)
            scals[i] = (1.0 / operat);
        else {
            grib_context_log(context_, GRIB_LOG_WARNING,
                             "%s: Problem with operator div by zero at index %d of %d", class_name_, i, maxv);
            scals[i] = 0;
        }
    }

    /*
    printf("UNPACKING LAPLACE=%.20f\n",laplacianOperator);
    printf("packed offset=%ld\n",packed_offset);
    for(i=0;i<maxv;i++)
        printf("scals[%d]=%g\n",i,scals[i]);*/

    i = 0;

    while (maxv > 0) {
        lup = mmax;
        if (sub_k >= 0) {
            for (hcount = 0; hcount < sub_k + 1; hcount++) {
                val[i++] = decode_float(grib_decode_unsigned_long(hres, &hpos, 8 * bytes));
                val[i++] = decode_float(grib_decode_unsigned_long(hres, &hpos, 8 * bytes));

                if (GRIBEX_sh_bug_present && hcount == sub_k) {
                    /*  bug in ecmwf data, last row (K+1)is scaled but should not */
                    val[i - 2] *= scals[lup];
                    val[i - 1] *= scals[lup];
                }
                lup++;
            }
            sub_k--;
        }

        pscals = scals + lup;
        pval   = val + i;
#if FAST_BIG_ENDIAN
        grib_decode_double_array_complex(lres,
                                         &lpos, bits_per_value,
                                         reference_value, s, pscals, (maxv - hcount) * 2, pval);
        i += (maxv - hcount) * 2;
#else
        (void)pscals; /* suppress gcc warning */
        (void)pval;   /* suppress gcc warning */
        for (lcount = hcount; lcount < maxv; lcount++) {
            val[i++] = d * (T)((grib_decode_unsigned_long(lres, &lpos, bits_per_value) * s) + reference_value) * scals[lup];
            val[i++] = d * (T)((grib_decode_unsigned_long(lres, &lpos, bits_per_value) * s) + reference_value) * scals[lup];
            /* These values should always be zero, but as they are packed,
               it is necessary to force them back to zero */
            if (mmax == 0)
                val[i - 1] = 0;
            lup++;
        }
#endif

        maxv--;
        hcount = 0;
        mmax++;
    }

    //ECCODES_ASSERT(*len >= i);
    if (*len < i) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s::%s: Invalid values *len=%zu and i=%zu.",
                         class_name_, __func__, *len, i);
        grib_context_log(context_, GRIB_LOG_ERROR, "Make sure your array is large enough.");
        ret = GRIB_ARRAY_TOO_SMALL;
    } else {
        *len = i;
    }

    grib_context_free(context_, scals);

    return ret;
}

int grib_accessor_data_complex_packing_t::unpack_double(double* val, size_t* len)
{
    return unpack_real<double>(val, len);
}

int grib_accessor_data_complex_packing_t::unpack_float(float* val, size_t* len)
{
    // TODO(maee): See ECC-1579
    // Investigate why results are not bit-identical

    // return unpack<float>(a, val, len);

    size_t size  = *len;
    double* val8 = (double*)grib_context_malloc(context_, size * (sizeof(double)));
    if (!val8) return GRIB_OUT_OF_MEMORY;

    int err = unpack_real<double>(val8, len);
    if (err) {
        grib_context_free(context_, val8);
        return err;
    }

    for (size_t i = 0; i < size; i++)
        val[i] = val8[i];
    grib_context_free(context_, val8);

    return GRIB_SUCCESS;
}
