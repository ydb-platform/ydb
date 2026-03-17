/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_padtoeven.h"

grib_accessor_padtoeven_t _grib_accessor_padtoeven{};
grib_accessor* grib_accessor_padtoeven = &_grib_accessor_padtoeven;

size_t grib_accessor_padtoeven_t::preferred_size(int from_handle)
{
    long offset = 0;
    long length = 0;
    long seclen;

    grib_get_long_internal(grib_handle_of_accessor(this), section_offset_, &offset);
    grib_get_long_internal(grib_handle_of_accessor(this), section_length_, &length);

    if ((length % 2) && from_handle) {
        //  grib_context_log(context_ ,
        //       GRIB_LOG_ERROR,"GRIB message has an odd length section (%ld, %s)",
        //       (long)length,name_ );
        return 0;
    }

    /* printf("EVEN %ld %ld\n",(long) offset_ ,(long) offset);*/
    seclen = offset_ - offset;

    return (seclen % 2) ? 1 : 0;
}

void grib_accessor_padtoeven_t::init(const long len, grib_arguments* args)
{
    grib_accessor_padding_t::init(len, args);

    section_offset_ = args->get_name(grib_handle_of_accessor(this), 0);
    section_length_ = args->get_name(grib_handle_of_accessor(this), 1);

    length_ = preferred_size(1);
}
