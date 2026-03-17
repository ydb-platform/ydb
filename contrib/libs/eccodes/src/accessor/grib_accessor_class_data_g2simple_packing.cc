/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g2simple_packing.h"
#include "grib_scaling.h"

grib_accessor_data_g2simple_packing_t _grib_accessor_data_g2simple_packing{};
grib_accessor* grib_accessor_data_g2simple_packing = &_grib_accessor_data_g2simple_packing;

void grib_accessor_data_g2simple_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
    edition_ = 2;
}

int grib_accessor_data_g2simple_packing_t::value_count(long* n_vals)
{
    *n_vals = 0;
    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, n_vals);
}

int grib_accessor_data_g2simple_packing_t::pack_double(const double* cval, size_t* len)
{
    // grib_accessor* super                = *(cclass_ ->super);
    size_t n_vals             = *len;
    double reference_value    = 0;
    long binary_scale_factor  = 0;
    long bits_per_value       = 0;
    long decimal_scale_factor = 0;
    double decimal            = 1;
    size_t buflen             = 0;
    unsigned char* buf        = NULL;
    unsigned char* encoded    = NULL;
    double divisor            = 1;
    long off                  = 0;
    int ret                   = 0;
    double units_factor       = 1.0;
    double units_bias         = 0.0;
    double* val               = (double*)cval;
    int i;
    grib_context* c = context_;

    if (*len == 0) {
        grib_buffer_replace(this, NULL, 0, 1, 1);
        return GRIB_SUCCESS;
    }

    if (ret == GRIB_SUCCESS)
        ret = grib_set_long_internal(grib_handle_of_accessor(this), number_of_values_, *len);

    if (ret != GRIB_SUCCESS)
        return ret;

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

    /* IEEE packing */
    if (c->ieee_packing) {
        grib_handle* h = grib_handle_of_accessor(this);
        long precision = 0; /* Either 1(=32 bits) or 2(=64 bits) */
        size_t lenstr  = 10;
        if ((ret = codes_check_grib_ieee_packing_value(c->ieee_packing)) != GRIB_SUCCESS)
            return ret;
        precision = c->ieee_packing == 32 ? 1 : 2;
        if ((ret = grib_set_string(h, "packingType", "grid_ieee", &lenstr)) != GRIB_SUCCESS)
            return ret;
        if ((ret = grib_set_long(h, "precision", precision)) != GRIB_SUCCESS)
            return ret;

        return grib_set_double_array(h, "values", val, *len);
    }

    ret = grib_accessor_data_simple_packing_t::pack_double(cval, len);
    switch (ret) {
        case GRIB_CONSTANT_FIELD:
            grib_buffer_replace(this, NULL, 0, 1, 1);
            return GRIB_SUCCESS;
        case GRIB_SUCCESS:
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "GRIB2 simple packing: unable to set values (%s)", grib_get_error_message(ret));
            return ret;
    }

    if ((ret = grib_get_double_internal(grib_handle_of_accessor(this), reference_value_, &reference_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return ret;

    decimal = codes_power<double>(decimal_scale_factor, 10);
    divisor = codes_power<double>(-binary_scale_factor, 2);

    buflen  = (((bits_per_value * n_vals) + 7) / 8) * sizeof(unsigned char);
    buf     = (unsigned char*)grib_context_buffer_malloc_clear(context_, buflen);
    encoded = buf;

    grib_encode_double_array(n_vals, val, bits_per_value, reference_value, decimal, divisor, encoded, &off);

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "grib_accessor_data_g2simple_packing_t : pack_double : packing %s, %d values", name_, n_vals);

    grib_buffer_replace(this, buf, buflen, 1, 1);

    grib_context_buffer_free(context_, buf);

    return ret;
}

int grib_accessor_data_g2simple_packing_t::pack_bytes(const unsigned char* val, size_t* len)
{
    size_t length = *len;
    grib_buffer_replace(this, val, length, 1, 1);
    return GRIB_SUCCESS;
}
