/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_count_missing.h"

grib_accessor_count_missing_t _grib_accessor_count_missing{};
grib_accessor* grib_accessor_count_missing = &_grib_accessor_count_missing;

static const unsigned char bitsoff[] = {
    8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 4, 7,
    6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 7, 6,
    6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 6, 5, 5,
    4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 7, 6, 6, 5,
    6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 6, 5, 5, 4, 5,
    4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 6, 5, 5, 4, 5, 4,
    4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 5, 4, 4, 3, 4, 3, 3,
    2, 4, 3, 3, 2, 3, 2, 2, 1, 7, 6, 6, 5, 6, 5, 5, 4,
    6, 5, 5, 4, 5, 4, 4, 3, 6, 5, 5, 4, 5, 4, 4, 3, 5,
    4, 4, 3, 4, 3, 3, 2, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4,
    4, 3, 4, 3, 3, 2, 5, 4, 4, 3, 4, 3, 3, 2, 4, 3, 3,
    2, 3, 2, 2, 1, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4, 4, 3,
    4, 3, 3, 2, 5, 4, 4, 3, 4, 3, 3, 2, 4, 3, 3, 2, 3,
    2, 2, 1, 5, 4, 4, 3, 4, 3, 3, 2, 4, 3, 3, 2, 3, 2,
    2, 1, 4, 3, 3, 2, 3, 2, 2, 1, 3, 2, 2, 1, 2, 1, 1,
    0
};

void grib_accessor_count_missing_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_long_t::init(len, arg);
    int n          = 0;
    grib_handle* h = grib_handle_of_accessor(this);
    length_        = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    bitmap_                     = arg->get_name(h, n++);
    unusedBitsInBitmap_         = arg->get_name(h, n++);
    numberOfDataPoints_         = arg->get_name(h, n++);
    missingValueManagementUsed_ = arg->get_name(h, n++); /* Can be NULL */
}

static const int used[] = { 0, 1, 3, 7, 15, 31, 63, 127, 255 };

static int get_count_of_missing_values(grib_handle* h, long* p_count_of_missing)
{
    int err               = 0;
    long count_of_missing = 0;
    size_t vsize = 0, ii = 0;
    double* values = NULL;
    double mv      = 0;
    if ((err = grib_get_double(h, "missingValue", &mv)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_size(h, "values", &vsize)) != GRIB_SUCCESS)
        return err;
    values = (double*)grib_context_malloc(h->context, vsize * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;
    if ((err = grib_get_double_array(h, "values", values, &vsize)) != GRIB_SUCCESS)
        return err;
    for (ii = 0; ii < vsize; ii++) {
        if (values[ii] == mv) ++count_of_missing;
    }
    grib_context_free(h->context, values);
    *p_count_of_missing = count_of_missing;

    return GRIB_SUCCESS;
}
int grib_accessor_count_missing_t::unpack_long(long* val, size_t* len)
{
    unsigned char* p;
    int i;
    long size               = 0;
    long offset             = 0;
    long unusedBitsInBitmap = 0;
    long numberOfDataPoints = 0;
    grib_handle* h          = grib_handle_of_accessor(this);
    grib_accessor* bitmap   = grib_find_accessor(h, bitmap_);

    *val = 0; /* By default assume none are missing */
    *len = 1;
    if (!bitmap) {
        long mvmu = 0;
        if (missingValueManagementUsed_ &&
            grib_get_long(h, missingValueManagementUsed_, &mvmu) == GRIB_SUCCESS && mvmu != 0) {
            /* ECC-523: No bitmap. Missing values are encoded in the Data Section.
             * So we must decode all the data values and count how many are missing
             */
            long count_of_missing = 0;
            if (get_count_of_missing_values(h, &count_of_missing) == GRIB_SUCCESS) {
                *val = count_of_missing;
            }
        }
        return GRIB_SUCCESS;
    }

    size   = bitmap->byte_count();
    offset = bitmap->byte_offset();
    if (grib_get_long(h, unusedBitsInBitmap_, &unusedBitsInBitmap) != GRIB_SUCCESS) {
        if (grib_get_long(h, numberOfDataPoints_, &numberOfDataPoints) != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR, "Unable to count missing values");
            return GRIB_INTERNAL_ERROR;
        }
        unusedBitsInBitmap = size * 8 - numberOfDataPoints;
        if (unusedBitsInBitmap < 0) {
            grib_context_log(context_, GRIB_LOG_ERROR, "Inconsistent number of bitmap points: Check the bitmap and data sections!");
            grib_context_log(context_, GRIB_LOG_ERROR, "Bitmap size=%ld, numberOfDataPoints=%ld", size * 8, numberOfDataPoints);
            return GRIB_DECODING_ERROR;
        }
    }

    p = h->buffer->data + offset;

    size -= unusedBitsInBitmap / 8;
    unusedBitsInBitmap = unusedBitsInBitmap % 8;

    for (i = 0; i < size - 1; i++)
        *val += bitsoff[*(p++)];

    *val += bitsoff[(*p) | used[unusedBitsInBitmap]];

    return GRIB_SUCCESS;
}

int grib_accessor_count_missing_t::value_count(long* count)
{
    *count = 1;
    return 0;
}
