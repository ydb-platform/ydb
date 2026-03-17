/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_jpeg2000_packing.h"

grib_accessor_data_jpeg2000_packing_t _grib_accessor_data_jpeg2000_packing{};
grib_accessor* grib_accessor_data_jpeg2000_packing = &_grib_accessor_data_jpeg2000_packing;

static int first = 1;

#define JASPER_LIB   1
#define OPENJPEG_LIB 2

void grib_accessor_data_jpeg2000_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_simple_packing_t::init(v, args);
    const char* user_lib = NULL;
    grib_handle* hand    = grib_handle_of_accessor(this);

    jpeg_lib_                 = 0;
    type_of_compression_used_ = args->get_name(hand, carg_++);
    target_compression_ratio_ = args->get_name(hand, carg_++);
    ni_                       = args->get_name(hand, carg_++);
    nj_                       = args->get_name(hand, carg_++);
    list_defining_points_     = args->get_name(hand, carg_++);
    number_of_data_points_    = args->get_name(hand, carg_++);
    scanning_mode_            = args->get_name(hand, carg_++);
    edition_                  = 2;
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;

#if HAVE_LIBJASPER
    jpeg_lib_ = JASPER_LIB;
#elif HAVE_LIBOPENJPEG
    jpeg_lib_ = OPENJPEG_LIB;
#endif

    if ((user_lib = codes_getenv("ECCODES_GRIB_JPEG")) != NULL) {
        if (!strcmp(user_lib, "jasper")) {
            jpeg_lib_ = JASPER_LIB;
        }
        else if (!strcmp(user_lib, "openjpeg")) {
            jpeg_lib_ = OPENJPEG_LIB;
        }
    }

    if (context_->debug) {
        switch (jpeg_lib_) {
            case 0:
                fprintf(stderr, "ECCODES DEBUG jpeg2000_packing: jpeg_lib not set!\n");
                break;
            case JASPER_LIB:
                fprintf(stderr, "ECCODES DEBUG jpeg2000_packing: using JASPER_LIB\n");
                break;
            case OPENJPEG_LIB:
                fprintf(stderr, "ECCODES DEBUG jpeg2000_packing: using OPENJPEG_LIB\n");
                break;
            default:
                ECCODES_ASSERT(0);
                break;
        }
    }

    dump_jpg_ = codes_getenv("ECCODES_GRIB_DUMP_JPG_FILE");
    if (dump_jpg_) {
        if (first) {
            printf("GRIB JPEG dumping to %s\n", dump_jpg_);
            first = 0;
        }
    }
}

int grib_accessor_data_jpeg2000_packing_t::value_count(long* n_vals)
{
    *n_vals = 0;

    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, n_vals);
}

#define EXTRA_BUFFER_SIZE 10240

#if HAVE_JPEG
int grib_accessor_data_jpeg2000_packing_t::unpack_float(float* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_data_jpeg2000_packing_t::unpack_double(double* val, size_t* len)
{
    int err            = GRIB_SUCCESS;
    grib_handle* hand  = grib_handle_of_accessor(this);

    size_t i           = 0;
    size_t buflen      = byte_count();
    double bscale      = 0;
    double dscale      = 0;
    unsigned char* buf = NULL;
    size_t n_vals      = 0;
    long nn            = 0;

    long binary_scale_factor  = 0;
    long decimal_scale_factor = 0;
    double reference_value    = 0;
    long bits_per_value       = 0;
    double units_factor       = 1.0;
    double units_bias         = 0.0;

    n_vals = 0;
    err    = value_count(&nn);
    n_vals = nn;
    if (err)
        return err;

    if (units_factor_)
        grib_get_double_internal(hand, units_factor_, &units_factor);

    if (units_bias_)
        grib_get_double_internal(hand, units_bias_, &units_bias);

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(hand, decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    dirty_ = 0;

    bscale = codes_power<double>(binary_scale_factor, 2);
    dscale = codes_power<double>(-decimal_scale_factor, 10);

    /* TODO: This should be called upstream */
    if (*len < n_vals)
        return GRIB_ARRAY_TOO_SMALL;

    /* Special case */

    if (bits_per_value == 0) {
        for (i = 0; i < n_vals; i++)
            val[i] = reference_value;
        *len = n_vals;
        return GRIB_SUCCESS;
    }

    buf = (unsigned char*)grib_handle_of_accessor(this)->buffer->data;
    buf += byte_offset();
    switch (jpeg_lib_) {
        case OPENJPEG_LIB:
            if ((err = grib_openjpeg_decode(context_, buf, &buflen, val, &n_vals)) != GRIB_SUCCESS)
                return err;
            break;
        case JASPER_LIB:
            if ((err = grib_jasper_decode(context_, buf, &buflen, val, &n_vals)) != GRIB_SUCCESS)
                return err;
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "Unable to unpack. Invalid JPEG library.\n");
            return GRIB_DECODING_ERROR;
    }

    *len = n_vals;

    for (i = 0; i < n_vals; i++) {
        val[i] = (val[i] * bscale + reference_value) * dscale;
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

    return err;
}

int grib_accessor_data_jpeg2000_packing_t::pack_double(const double* cval, size_t* len)
{
    size_t n_vals = *len;
    int err       = 0;
    double reference_value     = 0;
    long binary_scale_factor   = 0;
    long bits_per_value        = 0;
    long decimal_scale_factor  = 0;
    double decimal             = 1;
    size_t simple_packing_size = 0;
    unsigned char* buf         = NULL;
    double divisor             = 1;
    long width;
    long height;
    long ni;
    long nj;
    long target_compression_ratio;
    long type_of_compression_used;
    long scanning_mode;
    long list_defining_points;
    long number_of_data_points;
    int ret = 0;
    j2k_encode_helper helper;
    double units_factor     = 1.0;
    double units_bias       = 0.0;
    double* val             = (double*)cval;

    dirty_ = 1;

    if (*len == 0) {
        grib_buffer_replace(this, NULL, 0, 1, 1);
        return GRIB_SUCCESS;
    }

    if (units_factor_ &&
        (grib_get_double_internal(grib_handle_of_accessor(this), units_factor_, &units_factor) == GRIB_SUCCESS)) {
        grib_set_double_internal(grib_handle_of_accessor(this), units_factor_, 1.0);
    }

    if (units_bias_ &&
        (grib_get_double_internal(grib_handle_of_accessor(this), units_bias_, &units_bias) == GRIB_SUCCESS)) {
        grib_set_double_internal(grib_handle_of_accessor(this), units_bias_, 0.0);
    }

    if (units_factor != 1.0) {
        if (units_bias != 0.0)
            for (size_t i = 0; i < n_vals; i++)
                val[i] = val[i] * units_factor + units_bias;
        else
            for (size_t i = 0; i < n_vals; i++)
                val[i] *= units_factor;
    }
    else if (units_bias != 0.0)
        for (size_t i = 0; i < n_vals; i++)
            val[i] += units_bias;

    ret = grib_accessor_data_simple_packing_t::pack_double(val, len);

    switch (ret) {
        case GRIB_CONSTANT_FIELD:
            grib_buffer_replace(this, NULL, 0, 1, 1);
            err = grib_set_long_internal(grib_handle_of_accessor(this), number_of_values_, *len);
            return err;
        case GRIB_SUCCESS:
            break;
        default:
            grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: Unable to compute packing parameters", class_name_, __func__);
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

    decimal = codes_power<double>(decimal_scale_factor, 10);
    divisor = codes_power<double>(-binary_scale_factor, 2);

    simple_packing_size = (((bits_per_value * n_vals) + 7) / 8) * sizeof(unsigned char);
    buf                 = (unsigned char*)grib_context_malloc_clear(context_, simple_packing_size + EXTRA_BUFFER_SIZE);
    if (!buf) {
        err = GRIB_OUT_OF_MEMORY;
        goto cleanup;
    }

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), ni_, &ni)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), nj_, &nj)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), type_of_compression_used_, &type_of_compression_used)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), target_compression_ratio_, &target_compression_ratio)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), scanning_mode_, &scanning_mode)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), list_defining_points_, &list_defining_points)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), number_of_data_points_, &number_of_data_points)) != GRIB_SUCCESS)
        return err;

    width  = ni;
    height = nj;

    if ((scanning_mode & (1 << 5)) != 0) {
        long tmp = width;
        width    = height;
        height   = tmp;
    }

    /* The grid is not regular */
    if (list_defining_points != 0) {
        width  = *len;
        height = 1;
    }

    /* There is a bitmap */
    if (*len != number_of_data_points) {
        width  = *len;
        height = 1;
    }

    if (width * height != *len) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s %s: width=%ld height=%ld len=%zu. width*height should equal len!",
                         class_name_, __func__, width, height, *len);
        /* ECC-802: We cannot bomb out here as the user might have changed Ni/Nj and the packingType
         * but has not yet submitted the new data values. So len will be out of sync!
         * So issue a warning but proceed.
         */
        /*return GRIB_INTERNAL_ERROR;*/
        grib_context_free(context_, buf);
        return GRIB_SUCCESS;
    }

    switch (type_of_compression_used) {
        case 0:  // Lossless
            if (target_compression_ratio != 255) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "%s %s: When %s=0 (Lossless), %s must be set to 255",
                                 class_name_, __func__, type_of_compression_used, target_compression_ratio_);
                return GRIB_ENCODING_ERROR;
            }
            helper.compression = 0;
            break;

        case 1:  // Lossy
            if (target_compression_ratio == 255 || target_compression_ratio == 0) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "%s %s: When %s=1 (Lossy), %s must be specified",
                                 class_name_, __func__, type_of_compression_used, target_compression_ratio_);
                return GRIB_ENCODING_ERROR;
            }
            ECCODES_ASSERT(target_compression_ratio != 255);
            ECCODES_ASSERT(target_compression_ratio != 0);
            helper.compression = target_compression_ratio;
            break;

        default:
            err = GRIB_NOT_IMPLEMENTED;
            goto cleanup;
    }

    helper.jpeg_buffer = buf;
    helper.width       = width;
    helper.height      = height;

    /* See GRIB-438 */
    if (bits_per_value == 0) {
        const long bits_per_value_adjusted = 1;
        grib_context_log(context_, GRIB_LOG_DEBUG,
                         "%s (%s) : bits per value was zero, changed to %ld",
                         class_name_, jpeg_lib_ == OPENJPEG_LIB ? "openjpeg" : "jasper", bits_per_value_adjusted);
        bits_per_value = bits_per_value_adjusted;
    }
    helper.bits_per_value = bits_per_value;

    helper.buffer_size     = simple_packing_size + EXTRA_BUFFER_SIZE;
    helper.values          = val;
    helper.no_values       = n_vals;
    helper.reference_value = reference_value;
    helper.divisor         = divisor;
    helper.decimal         = decimal;
    helper.jpeg_length     = 0;

    switch (jpeg_lib_) {
        case OPENJPEG_LIB:
            if ((err = grib_openjpeg_encode(context_, &helper)) != GRIB_SUCCESS)
                goto cleanup;
            break;
        case JASPER_LIB:
            if ((err = grib_jasper_encode(context_, &helper)) != GRIB_SUCCESS)
                goto cleanup;
            break;
    }

    if (helper.jpeg_length > simple_packing_size)
        grib_context_log(context_, GRIB_LOG_WARNING,
                         "%s (%s) : jpeg data (%ld) larger than input data (%ld)",
                         class_name_, jpeg_lib_ == OPENJPEG_LIB ? "openjpeg" : "jasper",
                         helper.jpeg_length, simple_packing_size);

    ECCODES_ASSERT(helper.jpeg_length <= helper.buffer_size);

    if (dump_jpg_) {
        FILE* f = fopen(dump_jpg_, "w");
        if (f) {
            if (fwrite(helper.jpeg_buffer, helper.jpeg_length, 1, f) != 1)
                perror(dump_jpg_);
            if (fclose(f) != 0)
                perror(dump_jpg_);
        }
        else
            perror(dump_jpg_);
    }

    grib_buffer_replace(this, helper.jpeg_buffer, helper.jpeg_length, 1, 1);

cleanup:

    grib_context_free(context_, buf);

    if (err == GRIB_SUCCESS)
        err = grib_set_long_internal(grib_handle_of_accessor(this), number_of_values_, *len);
    return err;
}
#else

static void print_error_feature_not_enabled(grib_context* c)
{
    grib_context_log(c, GRIB_LOG_ERROR,
                     "JPEG support not enabled. Please rebuild with -DENABLE_JPG=ON");
}
int grib_accessor_data_jpeg2000_packing_t::unpack_float(float* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_jpeg2000_packing_t::unpack_double(double* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}

int grib_accessor_data_jpeg2000_packing_t::pack_double(const double* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}

#endif

int grib_accessor_data_jpeg2000_packing_t::unpack_double_element(size_t idx, double* val)
{
    grib_handle* hand      = grib_handle_of_accessor(this);
    size_t size            = 0;
    double* values         = NULL;
    int err                = 0;
    double reference_value = 0;
    long bits_per_value    = 0;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    /* Special case of constant field */
    if (bits_per_value == 0) {
        *val = reference_value;
        return GRIB_SUCCESS;
    }

    /* GRIB-564: The index idx relates to codedValues NOT values! */
    err = grib_get_size(hand, "codedValues", &size);
    if (err)
        return err;
    if (idx > size)
        return GRIB_INVALID_ARGUMENT;

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

int grib_accessor_data_jpeg2000_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    size_t size = 0, i = 0;
    double* values         = NULL;
    int err                = 0;
    double reference_value = 0;
    long bits_per_value    = 0;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    /* Special case of constant field */
    if (bits_per_value == 0) {
        for (i = 0; i < len; i++)
            val_array[i] = reference_value;
        return GRIB_SUCCESS;
    }

    /* GRIB-564: The indexes in index_array relate to codedValues NOT values! */
    err = grib_get_size(hand, "codedValues", &size);
    if (err) return err;

    for (i = 0; i < len; i++) {
        if (index_array[i] > size) return GRIB_INVALID_ARGUMENT;
    }

    values = (double*)grib_context_malloc_clear(context_, size * sizeof(double));
    err    = grib_get_double_array(hand, "codedValues", values, &size);
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
