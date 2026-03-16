/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1_half_byte_codeflag.h"

grib_accessor_g1_half_byte_codeflag_t _grib_accessor_g1_half_byte_codeflag{};
grib_accessor* grib_accessor_g1_half_byte_codeflag = &_grib_accessor_g1_half_byte_codeflag;

void grib_accessor_g1_half_byte_codeflag_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
}

void grib_accessor_g1_half_byte_codeflag_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_long(this, NULL);
}

int grib_accessor_g1_half_byte_codeflag_t::unpack_long(long* val, size_t* len)
{
    unsigned char dat = 0;
    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s it contains %d values ", name_, 1);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }
    dat = grib_handle_of_accessor(this)->buffer->data[offset_] & 0x0f;

    *val = dat;
    *len = 1;
    return GRIB_SUCCESS;
}

int grib_accessor_g1_half_byte_codeflag_t::pack_long(const long* val, size_t* len)
{
    int ret = 0;
    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s it contains %d values ", name_, 1);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }
    /*  printf("HALF BYTE pack long %ld %02x\n",*val,grib_handle_of_accessor(this)->buffer->data[offset_ ]);*/
    grib_handle_of_accessor(this)->buffer->data[offset_] = (parent_->h->buffer->data[offset_] & 0xf0) | (*val & 0x0f);
    /*  printf("HALF BYTE pack long %ld %02x\n",*val,grib_handle_of_accessor(this)->buffer->data[offset_ ]);*/

    *len = 1;
    return ret;
}

long grib_accessor_g1_half_byte_codeflag_t::get_native_type()
{
    return GRIB_TYPE_LONG;
}
