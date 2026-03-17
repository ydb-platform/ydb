/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1monthlydate.h"

grib_accessor_g1monthlydate_t _grib_accessor_g1monthlydate{};
grib_accessor* grib_accessor_g1monthlydate = &_grib_accessor_g1monthlydate;

void grib_accessor_g1monthlydate_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n = 0;

    date_ = c->get_name(grib_handle_of_accessor(this), n++);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_g1monthlydate_t::unpack_long(long* val, size_t* len)
{
    long date = 0;

    grib_get_long_internal(grib_handle_of_accessor(this), date_, &date);

    date /= 100;
    date *= 100;
    date += 1;

    *val = date;

    return GRIB_SUCCESS;
}
