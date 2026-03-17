/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_bit.h"

grib_accessor_bit_t _grib_accessor_bit{};
grib_accessor* grib_accessor_bit = &_grib_accessor_bit;

void grib_accessor_bit_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_long_t::init(len, arg);
    length_    = 0;
    owner_     = arg->get_name(grib_handle_of_accessor(this), 0);
    bit_index_ = arg->get_long(grib_handle_of_accessor(this), 1);
}

int grib_accessor_bit_t::unpack_long(long* val, size_t* len)
{
    int ret   = 0;
    long data = 0;

    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_bit_t: unpack_long: Wrong size for %s, it contains %d values ", name_, 1);
        *len = 1;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), owner_, &data)) != GRIB_SUCCESS) {
        *len = 0;
        return ret;
    }

    if (data & (1 << bit_index_))
        *val = 1;
    else
        *val = 0;

    *len = 1;
    return GRIB_SUCCESS;
}

int grib_accessor_bit_t::pack_long(const long* val, size_t* len)
{
    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_bit_t: pack_long: At least one value to pack for %s", name_);
        *len = 1;
        return GRIB_ARRAY_TOO_SMALL;
    }

    grib_accessor* owner = grib_find_accessor(grib_handle_of_accessor(this), owner_);
    if (!owner) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_bit_t: Cannot get the owner %s for computing the bit value of %s",
                         owner_, name_);
        *len = 0;
        return GRIB_NOT_FOUND;
    }

    unsigned char* mdata = grib_handle_of_accessor(this)->buffer->data;
    mdata += owner->byte_offset();
    /* Note: In the definitions, flagbit numbers go from 7 to 0 (the bit_index), while WMO convention is from 1 to 8 */
    if (context_->debug) {
        /* Print bit positions from 1 (MSB) */
        fprintf(stderr, "ECCODES DEBUG Setting bit %d in %s to %d\n", 8 - bit_index_, owner->name_, (*val > 0));
    }
    grib_set_bit(mdata, 7 - bit_index_, *val > 0);

    *len = 1;
    return GRIB_SUCCESS;
}
