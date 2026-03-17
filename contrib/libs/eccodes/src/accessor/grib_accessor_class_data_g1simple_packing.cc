/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g1simple_packing.h"
#include "grib_scaling.h"

grib_accessor_data_g1simple_packing_t _grib_accessor_data_g1simple_packing{};
grib_accessor* grib_accessor_data_g1simple_packing = &_grib_accessor_data_g1simple_packing;

void grib_accessor_data_g1simple_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);

    half_byte_    = args->get_name(grib_handle_of_accessor(this), carg_++);
    packingType_  = args->get_name(grib_handle_of_accessor(this), carg_++);
    ieee_packing_ = args->get_name(grib_handle_of_accessor(this), carg_++);
    precision_    = args->get_name(grib_handle_of_accessor(this), carg_++);
    edition_      = 1;
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_g1simple_packing_t::value_count(long* number_of_values)
{
    *number_of_values = 0;

    /* Special case for when values are cleared */
    /*if(length_ == 0)
    return 0;*/

    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, number_of_values);
}

int grib_accessor_data_g1simple_packing_t::pack_double(const double* cval, size_t* len)
{
    size_t n_vals             = *len;
    long half_byte            = 0;
    int ret                   = 0;
    long offsetdata           = 0;
    long offsetsection        = 0;
    double reference_value    = 0;
    long binary_scale_factor  = 0;
    long bits_per_value       = 0;
    long decimal_scale_factor = 0;
    double decimal            = 1;
    size_t buflen             = 0;
    unsigned char* buf        = NULL;
    unsigned char* encoded    = NULL;
    double divisor            = 1;
    int i;
    long off                   = 0;
    grib_context* c            = context_;
    grib_handle* h             = grib_handle_of_accessor(this);
    char* ieee_packing_s       = NULL;
    char* packingType_s        = NULL;
    char* precision_s          = NULL;
    double units_factor        = 1.0;
    double units_bias          = 0.0;
    double* val                = (double*)cval;
    double missingValue        = 9999.0;
    long constantFieldHalfByte = 0;
    int err                    = 0;

    if (*len != 0) {
        if (units_factor_ &&
            (grib_get_double_internal(grib_handle_of_accessor(this), units_factor_, &units_factor) == GRIB_SUCCESS)) {
            grib_set_double_internal(grib_handle_of_accessor(this), units_factor_, 1.0);
        }

        if (units_bias_ &&
            (grib_get_double_internal(grib_handle_of_accessor(this), units_bias_, &units_bias) == GRIB_SUCCESS)) {
            grib_set_double_internal(grib_handle_of_accessor(this), units_bias_, 0.0);
        }

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

        if (c->ieee_packing && ieee_packing_) {
            long precision = 0; /* Either 1(=32 bits) or 2(=64 bits) */
            size_t lenstr  = strlen(ieee_packing_);
            if ((ret = codes_check_grib_ieee_packing_value(c->ieee_packing)) != GRIB_SUCCESS)
                return ret;

            packingType_s  = grib_context_strdup(c, packingType_);
            ieee_packing_s = grib_context_strdup(c, ieee_packing_);
            precision_s    = grib_context_strdup(c, precision_);
            precision      = c->ieee_packing == 32 ? 1 : 2;

            if ((ret = grib_set_string(h, packingType_s, ieee_packing_s, &lenstr)) != GRIB_SUCCESS)
                return ret;
            if ((ret = grib_set_long(h, precision_s, precision)) != GRIB_SUCCESS)
                return ret;

            grib_context_free(c, packingType_s);
            grib_context_free(c, ieee_packing_s);
            grib_context_free(c, precision_s);
            return grib_set_double_array(h, "values", val, *len);
        }
    }

    ret = grib_accessor_data_simple_packing_t::pack_double(val, len);
    switch (ret) {
        case GRIB_CONSTANT_FIELD:
            ret = grib_get_long(grib_handle_of_accessor(this), "constantFieldHalfByte", &constantFieldHalfByte);
            if (ret)
                constantFieldHalfByte = 0;
            if ((ret = grib_set_long_internal(grib_handle_of_accessor(this), half_byte_, constantFieldHalfByte)) != GRIB_SUCCESS)
                return ret;
            ret = grib_buffer_replace(this, NULL, 0, 1, 1);
            if (ret != GRIB_SUCCESS) return ret;
            return GRIB_SUCCESS;

        case GRIB_NO_VALUES:
            ret = grib_get_long(grib_handle_of_accessor(this), "constantFieldHalfByte", &constantFieldHalfByte);
            if (ret)
                constantFieldHalfByte = 0;
            /* TODO move to def file */
            grib_get_double(grib_handle_of_accessor(this), "missingValue", &missingValue);
            if ((err = grib_set_double_internal(grib_handle_of_accessor(this), reference_value_, missingValue)) !=
                GRIB_SUCCESS)
                return err;
            if ((ret = grib_set_long_internal(grib_handle_of_accessor(this), binary_scale_factor_, binary_scale_factor)) != GRIB_SUCCESS)
                return ret;
            if ((ret = grib_set_long_internal(grib_handle_of_accessor(this), half_byte_, constantFieldHalfByte)) != GRIB_SUCCESS)
                return ret;
            ret = grib_buffer_replace(this, NULL, 0, 1, 1);
            if (ret != GRIB_SUCCESS) return ret;
            return GRIB_SUCCESS;

        case GRIB_INVALID_BPV:
            grib_context_log(context_, GRIB_LOG_ERROR, "Unable to compute packing parameters. Invalid bits per value");
            return ret;
        case GRIB_SUCCESS:
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "GRIB1 simple packing: unable to set values (%s)", grib_get_error_message(ret));
            return ret;
    }

    if ((ret = grib_get_double_internal(grib_handle_of_accessor(this), reference_value_, &reference_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), bits_per_value_, &bits_per_value)) !=
        GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), offsetdata_, &offsetdata)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), offsetsection_, &offsetsection)) != GRIB_SUCCESS)
        return ret;

    decimal = codes_power<double>(decimal_scale_factor, 10);
    divisor = codes_power<double>(-binary_scale_factor, 2);

    buflen = (((bits_per_value * n_vals) + 7) / 8) * sizeof(unsigned char);
    if ((buflen + (offsetdata - offsetsection)) % 2) {
        buflen++;
        /*
    length_ ++;
    grib_handle_of_accessor(this)->buffer->ulength++;
         */
    }
    half_byte = (buflen * 8) - ((*len) * bits_per_value);
    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "HALF byte: buflen=%d bits_per_value=%ld len=%d half_byte=%ld\n",
                     buflen, bits_per_value, *len, half_byte);

    ECCODES_ASSERT(half_byte <= 0x0f);

    if ((ret = grib_set_long_internal(grib_handle_of_accessor(this), half_byte_, half_byte)) != GRIB_SUCCESS)
        return ret;

    buf     = (unsigned char*)grib_context_buffer_malloc_clear(context_, buflen);
    encoded = buf;

    grib_encode_double_array(n_vals, val, bits_per_value, reference_value, decimal, divisor, encoded, &off);

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "grib_accessor_data_g1simple_packing_t : pack_double : packing %s, %d values", name_, n_vals);

    ret = grib_buffer_replace(this, buf, buflen, 1, 1);
    if (ret != GRIB_SUCCESS) return ret;

    grib_context_buffer_free(context_, buf);

    return GRIB_SUCCESS;
}
