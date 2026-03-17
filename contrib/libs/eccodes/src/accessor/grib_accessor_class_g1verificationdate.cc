/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1verificationdate.h"

grib_accessor_g1verificationdate_t _grib_accessor_g1verificationdate{};
grib_accessor* grib_accessor_g1verificationdate = &_grib_accessor_g1verificationdate;

void grib_accessor_g1verificationdate_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n = 0;

    date_ = c->get_name(grib_handle_of_accessor(this), n++);
    time_ = c->get_name(grib_handle_of_accessor(this), n++);
    step_ = c->get_name(grib_handle_of_accessor(this), n++);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_g1verificationdate_t::unpack_long(long* val, size_t* len)
{
    int ret    = 0;
    long date  = 0;
    long time  = 0;
    long cdate = 0;
    long step  = 0;
    long vtime = 0;
    long vdate = 0;
    long vd    = 0;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), date_, &date)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), time_, &time)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), step_, &step)) != GRIB_SUCCESS)
        return ret;

    time /= 100;

    cdate = (long)grib_date_to_julian(date);
    vtime = cdate * 24 + time + step;
    vd    = vtime / 24;
    vdate = grib_julian_to_date(vd);

    // printf("\n********\n date %d, time %d, step %d, vdate: %d, cdate %d, vd %d\n********\n", date, time, step, vdate, cdate, vd);

    if (*len < 1)
        return GRIB_ARRAY_TOO_SMALL;

    *val = vdate;

    // fprintf(stdout,"\n********\n %d cdate %d vd %d\n********\n", vdate, cdate, step);
    return GRIB_SUCCESS;
}
