/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g2bitmap.h"

grib_accessor_g2bitmap_t _grib_accessor_g2bitmap{};
grib_accessor* grib_accessor_g2bitmap = &_grib_accessor_g2bitmap;

void grib_accessor_g2bitmap_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_bitmap_t::init(len, arg);

    numberOfValues_ = arg->get_name(grib_handle_of_accessor(this), 4);
}

// For speed use a local static function
static GRIB_INLINE void set_bit_on(unsigned char* p, long* bitp)
{
    p += *bitp / 8;
    *p |= (1u << (7 - ((*bitp) % 8)));
    (*bitp)++;
}

int grib_accessor_g2bitmap_t::pack_double(const double* val, size_t* len)
{
    unsigned char* buf = NULL;
    size_t i;
    int err  = 0;
    long pos = 0;
    // long bmaplen       = 0;
    double miss_values = 0;
    size_t tlen        = (*len + 7) / 8;

    if ((err = grib_get_double_internal(grib_handle_of_accessor(this), missing_value_, &miss_values)) != GRIB_SUCCESS)
        return err;

    buf = (unsigned char*)grib_context_malloc_clear(context_, tlen);
    if (!buf)
        return GRIB_OUT_OF_MEMORY;
    pos = 0;
    for (i = 0; i < *len; i++) {
        if (val[i] == miss_values)
            pos++;
        else {
            // bmaplen++;
            set_bit_on(buf, &pos);
        }
    }

    if ((err = grib_set_long_internal(grib_handle_of_accessor(this), numberOfValues_, *len)) != GRIB_SUCCESS) {
        grib_context_free(context_, buf);
        return err;
    }

    grib_buffer_replace(this, buf, tlen, 1, 1);

    grib_context_free(context_, buf);

    return GRIB_SUCCESS;
}

int grib_accessor_g2bitmap_t::value_count(long* tlen)
{
    int err;
    *tlen = 0;

    err = grib_get_long_internal(grib_handle_of_accessor(this), numberOfValues_, tlen);
    return err;
}
