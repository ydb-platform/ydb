/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_dirty.h"

grib_accessor_dirty_t _grib_accessor_dirty{};
grib_accessor* grib_accessor_dirty = &_grib_accessor_dirty;

void grib_accessor_dirty_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    accessor_ = c->get_name(grib_handle_of_accessor(this), 0);
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;
    length_ = 0;
}

int grib_accessor_dirty_t::pack_long(const long* val, size_t* len)
{
    grib_accessor* x = grib_find_accessor(grib_handle_of_accessor(this), accessor_);

    if (x)
        x->dirty_ = *val;

    return GRIB_SUCCESS;
}

int grib_accessor_dirty_t::unpack_long(long* val, size_t* len)
{
    grib_accessor* x = grib_find_accessor(grib_handle_of_accessor(this), accessor_);

    if (x) {
        // printf("\ngrib_accessor_class_dirty_t::unpack_long: Getting dirty for %s\n", x->name);
        *val = x->dirty_;
    }
    else {
        *val = 1;
    }

    return GRIB_SUCCESS;
}
