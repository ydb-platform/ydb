/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_png_packing.h"
#include "grib_scaling.h"

#define PNG_ANYBITS

grib_accessor_data_png_packing_t _grib_accessor_data_png_packing{};
grib_accessor* grib_accessor_data_png_packing = &_grib_accessor_data_png_packing;

void grib_accessor_data_png_packing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_values_t::init(v, args);
    grib_handle* h = grib_handle_of_accessor(this);

    number_of_values_      = args->get_name(h, carg_++);
    reference_value_       = args->get_name(h, carg_++);
    binary_scale_factor_   = args->get_name(h, carg_++);
    decimal_scale_factor_  = args->get_name(h, carg_++);
    bits_per_value_        = args->get_name(h, carg_++);
    ni_                    = args->get_name(h, carg_++);
    nj_                    = args->get_name(h, carg_++);
    list_defining_points_  = args->get_name(h, carg_++);
    number_of_data_points_ = args->get_name(h, carg_++);
    scanning_mode_         = args->get_name(h, carg_++);
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_png_packing_t::value_count(long* n_vals)
{
    *n_vals = 0;
    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, n_vals);
}

#if HAVE_LIBPNG

    #include <png.h>

typedef struct png_read_callback_data
{
    unsigned char* buffer;
    size_t length;
    size_t offset;
} png_read_callback_data;

static void png_read_callback(png_structp png, png_bytep data, png_size_t length)
{
    png_read_callback_data* p = (png_read_callback_data*)png_get_io_ptr(png);
    ECCODES_ASSERT(p->offset + length <= p->length);
    memcpy(data, p->buffer + p->offset, length);
    p->offset += length;
}

static void png_write_callback(png_structp png, png_bytep data, png_size_t length)
{
    png_read_callback_data* p = (png_read_callback_data*)png_get_io_ptr(png);
    /* printf("p.offset=%zu  len=%zu p.len=%zu\n", p->offset, length, p->length); */
    /* ECCODES_ASSERT(p->offset + length <= p->length); */
    if (p->offset + length > p->length) {
        /* Errors handled through png_error() are fatal, meaning that png_error() should never return to its caller.
           Currently, this is handled via setjmp() and longjmp() */
        png_error(png, "Failed to write PNG data");
    }
    memcpy(p->buffer + p->offset, data, length);
    p->offset += length;
}

static void png_flush_callback(png_structp png)
{
    /* Empty */
}

int grib_accessor_data_png_packing_t::unpack_double(double* val, size_t* len)
{
    int err = GRIB_SUCCESS;
    int i, j;
    size_t buflen      = byte_count();
    double bscale      = 0;
    double dscale      = 0;
    unsigned char* buf = NULL;
    size_t n_vals      = 0;

    long binary_scale_factor  = 0;
    long decimal_scale_factor = 0;
    double reference_value    = 0;
    long bits_per_value       = 0;
    long bits8;
    long nn = 0;

    png_structp png = 0;
    png_infop info = 0, theEnd = 0;
    png_bytepp rows = 0;
    int interlace = 0, colour = 0, compression = 0, filter = 0, depth = 0;

    png_uint_32 width = 0, height = 0;

    png_read_callback_data callback_data;

    dirty_ = 0;

    err    = value_count(&nn);
    n_vals = nn;
    if (err) return err;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(grib_handle_of_accessor(this), reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

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

    if (png_sig_cmp(buf, 0, 8) != 0)
        return GRIB_INVALID_MESSAGE;

    png = png_create_read_struct(PNG_LIBPNG_VER_STRING, NULL,
                                 NULL, NULL);
    if (!png) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    info = png_create_info_struct(png);
    if (!info) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    theEnd = png_create_info_struct(png);
    if (!theEnd) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    if (setjmp(png_jmpbuf(png))) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    callback_data.buffer = buf;
    callback_data.offset = 0;
    callback_data.length = buflen;

    /* printf("buflen=%d\n",buflen); */

    png_set_read_fn(png, &callback_data, png_read_callback);
    png_read_png(png, info, PNG_TRANSFORM_IDENTITY, NULL);

    ECCODES_ASSERT(callback_data.offset == callback_data.length);

    rows = png_get_rows(png, info);

    png_get_IHDR(png, info,
                 &width, &height,
                 &depth, &colour,
                 &interlace,
                 &compression,
                 &filter);

    if (colour == PNG_COLOR_TYPE_RGB)
        depth = 24;
    if (colour == PNG_COLOR_TYPE_RGB_ALPHA)
        depth = 32;
    bits8 = ((bits_per_value + 7) / 8) * 8;

    #ifdef PNG_ANYBITS
    ECCODES_ASSERT(depth == bits8);
    #else
    ECCODES_ASSERT(bits_per_value % 8 == 0);
    #endif

    i = 0;

    /* printf("bscale=%g dscale=%g reference_value=%g\n",bscale,dscale,reference_value); */

    for (j = 0; j < height; j++) {
        png_byte* row = rows[j];
        long pos      = 0;
        int k;
        for (k = 0; k < width; k++)
            val[i++] = (double)(((grib_decode_unsigned_long(row, &pos, bits8) * bscale) + reference_value) * dscale);
    }
    /*-------------------------------------------*/
    *len = n_vals;

cleanup:
    if (png)
        png_destroy_read_struct(&png, info ? &info : NULL, theEnd ? &theEnd : NULL);
    return err;
}

static bool is_constant(const double* values, size_t n_vals)
{
    bool isConstant = true;
    double v        = 0;
    size_t i;
    for (i = 0; i < n_vals; i++) {
        if (i == 0)
            v = values[i];
        else if (v != values[i]) {
            isConstant = false;
            break;
        }
    }
    return isConstant;
}

int grib_accessor_data_png_packing_t::pack_double(const double* val, size_t* len)
{
    int err = GRIB_SUCCESS;
    bool is_constant_field = false;
    int i, j;
    size_t buflen = 0;

    unsigned char* buf     = NULL;
    unsigned char* encoded = NULL;
    size_t n_vals          = 0;

    long binary_scale_factor  = 0;
    long decimal_scale_factor = 0;
    double reference_value    = 0;
    long bits8;
    long bits_per_value = 0;
    double max, min;
    long bytes;
    double d;

    png_structp png = 0;
    png_infop info  = 0;
    png_bytepp rows = 0;
    int colour = 0, depth = 0;

    png_uint_32 width = 0, height = 0;
    unsigned char* p;
    double divisor;

    png_read_callback_data callback_data;
    long ni, nj;
    long scanning_mode;
    long list_defining_points;
    long number_of_data_points;

    dirty_ = 1;

    n_vals = *len;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(grib_handle_of_accessor(this), reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), binary_scale_factor_, &binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), decimal_scale_factor_, &decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    /* Special case */
    if (*len == 0) {
        grib_buffer_replace(this, NULL, 0, 1, 1);
        return GRIB_SUCCESS;
    }

    is_constant_field = is_constant(val, n_vals);
    if (!is_constant_field && bits_per_value == 0) {
        /* A non-constant field with bitsPerValue==0! */
        bits_per_value = 24; /* Set sane value */
    }

    if (is_constant_field) {
    #ifdef DEBUG
        for (i = 1; i < n_vals; i++) {
            ECCODES_ASSERT(val[i] == val[0]);
        }
    #endif
        if ((err = grib_set_double_internal(grib_handle_of_accessor(this), reference_value_, val[0])) != GRIB_SUCCESS)
            return err;

        {
            // Make sure we can decode it again
            double ref = 1e-100;
            grib_get_double_internal(grib_handle_of_accessor(this), reference_value_, &ref);
            if (ref != reference_value) {
                grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: %s (ref=%.10e != reference_value=%.10e)",
                                 class_name_, __func__, reference_value_, ref, reference_value);
                return GRIB_INTERNAL_ERROR;
            }
        }

        if ((err = grib_set_long_internal(grib_handle_of_accessor(this), number_of_values_, n_vals)) != GRIB_SUCCESS)
            return err;

        grib_buffer_replace(this, NULL, 0, 1, 1);

        return GRIB_SUCCESS;
    }

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), ni_, &ni)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), nj_, &nj)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), scanning_mode_, &scanning_mode)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), list_defining_points_, &list_defining_points)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), number_of_data_points_, &number_of_data_points)) != GRIB_SUCCESS)
        return err;

    width  = ni;
    height = nj;

    // jPointsAreConsecutive
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
                         "%s %s: width=%ld height=%ld len=%ld. width*height should equal len!",
                         class_name_, __func__, (long)width, (long)height, (long)*len);
        /* ECC-802: We cannot bomb out here as the user might have changed Ni/Nj and the packingType
         * but has not yet submitted the new data values. So len will be out of sync!
         * So issue a warning but proceed.
         */
        return GRIB_SUCCESS;
    }

    d = codes_power<double>(decimal_scale_factor, 10);

    max = val[0];
    min = max;
    for (i = 1; i < n_vals; i++) {
        if (val[i] > max)
            max = val[i];
        else if (val[i] < min)
            min = val[i];
    }
    min *= d;
    max *= d;

    if (grib_get_nearest_smaller_value(grib_handle_of_accessor(this), reference_value_, min, &reference_value) != GRIB_SUCCESS) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Unable to find nearest_smaller_value of %g for %s", min, reference_value_);
        return GRIB_INTERNAL_ERROR;
    }

    if (reference_value > min) {
        grib_context_log(context_, GRIB_LOG_ERROR, "reference_value=%g min_value=%g diff=%g",
                         reference_value, min, reference_value - min);
        return GRIB_INTERNAL_ERROR;
    }

    binary_scale_factor = grib_get_binary_scale_fact(max, reference_value, bits_per_value, &err);
    if (err) return err;
    divisor             = codes_power<double>(-binary_scale_factor, 2);

    #ifndef PNG_ANYBITS
    ECCODES_ASSERT(bits_per_value % 8 == 0);
    #endif
    bits8   = (bits_per_value + 7) / 8 * 8;
    encoded = (unsigned char*)grib_context_buffer_malloc_clear(context_, bits8 / 8 * n_vals);
    if (!encoded) {
        err = GRIB_OUT_OF_MEMORY;
        goto cleanup;
    }

    buflen = 0;
    p      = encoded;
    for (i = 0; i < n_vals; i++) {
        long blen                  = bits8;
        unsigned long unsigned_val = (unsigned long)((((val[i] * d) - (reference_value)) * divisor) + 0.5);
        while (blen >= 8) {
            blen -= 8;
            *p = (unsigned_val >> blen);
            p++;
            buflen++;
        }
    }
    /* buflen = n_vals*(bits_per_value/8); */
    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "grib_accessor_data_png_packing_t : pack_double : packing %s, %d values", name_, n_vals);
    buf = (unsigned char*)grib_context_buffer_malloc_clear(context_, buflen);

    if (!buf) {
        err = GRIB_OUT_OF_MEMORY;
        goto cleanup;
    }

    if ((err = grib_set_double_internal(grib_handle_of_accessor(this), reference_value_, reference_value)) != GRIB_SUCCESS)
        return err;

    {
        // Make sure we can decode it again
        double ref = 1e-100;
        grib_get_double_internal(grib_handle_of_accessor(this), reference_value_, &ref);
        if (ref != reference_value) {
            grib_context_log(context_, GRIB_LOG_ERROR, "%s %s: %s (ref=%.10e != reference_value=%.10e)",
                             class_name_, __func__, reference_value_, ref, reference_value);
            return GRIB_INTERNAL_ERROR;
        }
    }

    if ((err = grib_set_long_internal(grib_handle_of_accessor(this), binary_scale_factor_, binary_scale_factor)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_set_long_internal(grib_handle_of_accessor(this), decimal_scale_factor_, decimal_scale_factor)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), ni_, &ni)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), nj_, &nj)) != GRIB_SUCCESS)
        return err;

    png = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
    if (!png) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    info = png_create_info_struct(png);
    if (!info) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    if (setjmp(png_jmpbuf(png))) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    callback_data.buffer = buf;
    callback_data.offset = 0;
    callback_data.length = buflen;

    png_set_write_fn(png, &callback_data, png_write_callback, png_flush_callback);

    depth = bits8;

    colour = PNG_COLOR_TYPE_GRAY;
    if (bits8 == 24) {
        depth  = 8;
        colour = PNG_COLOR_TYPE_RGB;
    }

    if (bits8 == 32) {
        depth  = 8;
        colour = PNG_COLOR_TYPE_RGB_ALPHA;
    }

    png_set_IHDR(png, info, width, height,
                 depth, colour, PNG_INTERLACE_NONE,
                 PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);

    /*bytes=bit_depth/8;*/
    bytes = bits8 / 8;

    rows = (png_bytepp)grib_context_buffer_malloc_clear(context_, sizeof(png_bytep) * height);
    if (!rows) {
        err = GRIB_OUT_OF_MEMORY;
        goto cleanup;
    }

    for (j = 0; j < height; j++)
        rows[j] = &encoded[j * width * bytes];

    png_set_rows(png, info, rows);

    png_write_png(png, info, PNG_TRANSFORM_IDENTITY, NULL);

    ECCODES_ASSERT(callback_data.offset <= callback_data.length);

    grib_buffer_replace(this, buf, callback_data.offset, 1, 1);

cleanup:
    if (png)
        png_destroy_write_struct(&png, info ? &info : NULL);

    grib_context_buffer_free(context_, buf);
    grib_context_buffer_free(context_, encoded);
    grib_context_buffer_free(context_, rows);

    if (err == GRIB_SUCCESS)
        err = grib_set_long_internal(grib_handle_of_accessor(this), number_of_values_, *len);

    return err;
}

int grib_accessor_data_png_packing_t::unpack_double_element(size_t idx, double* val)
{
    /* The index idx relates to codedValues NOT values! */
    grib_handle* hand      = grib_handle_of_accessor(this);
    int err                = 0;
    size_t size            = 0;
    double reference_value = 0;
    long bits_per_value    = 0;
    double* values         = NULL;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    /* Special case of constant field */
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

int grib_accessor_data_png_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    /* The index idx relates to codedValues NOT values! */
    grib_handle* hand = grib_handle_of_accessor(this);
    int err           = 0;
    size_t size = 0, i = 0;
    double reference_value = 0;
    long bits_per_value    = 0;
    double* values         = NULL;

    if ((err = grib_get_long_internal(hand, bits_per_value_, &bits_per_value)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_double_internal(hand, reference_value_, &reference_value)) != GRIB_SUCCESS)
        return err;

    /* Special case of constant field */
    if (bits_per_value == 0) {
        for (i = 0; i < len; i++) {
            val_array[i] = reference_value;
        }
        return GRIB_SUCCESS;
    }

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
                     "PNG support not enabled. Please rebuild with -DENABLE_PNG=ON");
}

int grib_accessor_data_png_packing_t::unpack_double(double* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_png_packing_t::pack_double(const double* val, size_t* len)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_png_packing_t::unpack_double_element(size_t idx, double* val)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}
int grib_accessor_data_png_packing_t::unpack_double_element_set(const size_t* index_array, size_t len, double* val_array)
{
    print_error_feature_not_enabled(context_);
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}

#endif
