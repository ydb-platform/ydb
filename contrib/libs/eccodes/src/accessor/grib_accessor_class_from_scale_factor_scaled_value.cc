/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_from_scale_factor_scaled_value.h"
#include "ecc_numeric_limits.h"

grib_accessor_from_scale_factor_scaled_value_t _grib_accessor_from_scale_factor_scaled_value{};
grib_accessor* grib_accessor_from_scale_factor_scaled_value = &_grib_accessor_from_scale_factor_scaled_value;

void grib_accessor_from_scale_factor_scaled_value_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n             = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    scaleFactor_ = c->get_name(hand, n++);
    scaledValue_ = c->get_name(hand, n++);  // Can be scalar or array

    // ECC-979: Allow user to encode
    // flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_from_scale_factor_scaled_value_t::pack_double(const double* val, size_t* len)
{
    // See ECC-979 and ECC-1416
    // Evaluate scaleFactor and scaledValue_ from input double '*val'
    grib_handle* hand = grib_handle_of_accessor(this);
    int err           = 0;
    int64_t factor    = 0;
    int64_t value     = 0;
    double exact      = *val;             // the input
    int64_t maxval_value, maxval_factor;  // maximum allowable values
    int value_accessor_num_bits = 0, factor_accessor_num_bits = 0;
    grib_accessor *factor_accessor, *value_accessor;

    if (exact == 0) {
        if ((err = grib_set_long_internal(hand, scaleFactor_, 0)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_long_internal(hand, scaledValue_, 0)) != GRIB_SUCCESS)
            return err;
        return GRIB_SUCCESS;
    }

    if (exact == GRIB_MISSING_DOUBLE) {
        if ((err = grib_set_missing(hand, scaleFactor_)) != GRIB_SUCCESS)
            return err;
        if ((err = grib_set_missing(hand, scaledValue_)) != GRIB_SUCCESS)
            return err;
        return GRIB_SUCCESS;
    }

    factor_accessor = grib_find_accessor(hand, scaleFactor_);
    value_accessor  = grib_find_accessor(hand, scaledValue_);
    if (!factor_accessor || !value_accessor) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Could not access keys %s and %s", scaleFactor_, scaledValue_);
        return GRIB_ENCODING_ERROR;
    }
    value_accessor_num_bits  = value_accessor->length_ * 8;
    factor_accessor_num_bits = factor_accessor->length_ * 8;
    maxval_value             = NumericLimits<int64_t>::max(value_accessor_num_bits);  // exclude missing
    maxval_factor            = NumericLimits<int64_t>::max(factor_accessor_num_bits);  // exclude missing
    if (strcmp(factor_accessor->class_name_, "signed") == 0) {
        maxval_factor = (1UL << (factor_accessor_num_bits - 1)) - 1;
    }

    err = compute_scaled_value_and_scale_factor(exact, maxval_value, maxval_factor, &value, &factor);
    if (err) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Failed to compute %s and %s from %g", scaleFactor_, scaledValue_, exact);
        return err;
    }

    if ((err = grib_set_long_internal(hand, scaleFactor_, factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(hand, scaledValue_, value)) != GRIB_SUCCESS)
        return err;

    return GRIB_SUCCESS;
}

int grib_accessor_from_scale_factor_scaled_value_t::unpack_double(double* val, size_t* len)
{
    int err          = 0;
    long scaleFactor = 0, scaledValue = 0;
    grib_handle* hand = grib_handle_of_accessor(this);
    grib_context* c   = context_;
    size_t vsize      = 0;

    if ((err = grib_get_long_internal(hand, scaleFactor_, &scaleFactor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_size(hand, scaledValue_, &vsize)) != GRIB_SUCCESS)
        return err;

    if (vsize == 1) {
        if ((err = grib_get_long_internal(hand, scaledValue_, &scaledValue)) != GRIB_SUCCESS)
            return err;

        if (grib_is_missing(hand, scaledValue_, &err) && err == GRIB_SUCCESS) {
            *val = GRIB_MISSING_DOUBLE;
            *len = 1;
            return GRIB_SUCCESS;
        }
        else {
            // ECC-966: If scale factor is missing, print error and treat it as zero (as a fallback)
            if (grib_is_missing(hand, scaleFactor_, &err) && err == GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "unpack_double for %s: %s is missing! Using zero instead", name_, scaleFactor_);
                scaleFactor = 0;
            }
        }

        *val = scaledValue;

        // The formula is:
        //  real_value = scaled_value / pow(10, scale_factor)
        //
        while (scaleFactor < 0) {
            *val *= 10;
            scaleFactor++;
        }
        while (scaleFactor > 0) {
            *val /= 10;
            scaleFactor--;
        }

        if (err == GRIB_SUCCESS)
            *len = 1;
    }
    else {
        size_t i;
        long* lvalues = (long*)grib_context_malloc(c, vsize * sizeof(long));
        if (!lvalues)
            return GRIB_OUT_OF_MEMORY;
        if ((err = grib_get_long_array_internal(hand, scaledValue_, lvalues, &vsize)) != GRIB_SUCCESS) {
            grib_context_free(c, lvalues);
            return err;
        }
        for (i = 0; i < vsize; i++) {
            long sf = scaleFactor;
            val[i]  = lvalues[i];
            while (sf < 0) {
                val[i] *= 10;
                sf++;
            }
            while (sf > 0) {
                val[i] /= 10;
                sf--;
            }
        }
        *len = vsize;
        grib_context_free(c, lvalues);
    }

    return err;
}

int grib_accessor_from_scale_factor_scaled_value_t::is_missing()
{
    grib_handle* hand = grib_handle_of_accessor(this);
    int err           = 0;
    long scaleFactor = 0, scaledValue = 0;

    if ((err = grib_get_long_internal(hand, scaleFactor_, &scaleFactor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(hand, scaledValue_, &scaledValue)) != GRIB_SUCCESS)
        return err;

    return ((scaleFactor == GRIB_MISSING_LONG) || (scaledValue == GRIB_MISSING_LONG));
}

int grib_accessor_from_scale_factor_scaled_value_t::value_count(long* len)
{
    int err           = 0;
    grib_handle* hand = grib_handle_of_accessor(this);
    size_t vsize;

    if ((err = grib_get_size(hand, scaledValue_, &vsize)) != GRIB_SUCCESS)
        return err;
    *len = (long)vsize;
    return GRIB_SUCCESS;
}
