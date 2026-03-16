/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1_section4_length.h"

grib_accessor_g1_section4_length_t _grib_accessor_g1_section4_length{};
grib_accessor* grib_accessor_g1_section4_length = &_grib_accessor_g1_section4_length;

void grib_accessor_g1_section4_length_t::init(const long len, grib_arguments* args)
{
    grib_accessor_section_length_t::init(len, args);
    total_length_ = args->get_name(grib_handle_of_accessor(this), 0);
}

int grib_accessor_g1_section4_length_t::pack_long(const long* val, size_t* len)
{
    // Old implementation:
    //   Here we assume that the totalLength will be coded AFTER the section4 length, and
    //   the section4 length will be overwritten by the totalLength accessor for large GRIBs
    //    grib_accessor* super = *(cclass_ ->super);
    //    return super->pack_long(a,val,len);

    // Note: Do not directly call pack_long on base class
    // because in this special case we want to skip the checks.
    // So we call the helper function which has an extra argument
    return pack_long_unsigned_helper(val, len, /*check=*/0);
}

int grib_accessor_g1_section4_length_t::unpack_long(long* val, size_t* len)
{
    int ret           = 0;
    long total_length = 0, sec4_length = 0;

    if ((ret = grib_get_g1_message_size(grib_handle_of_accessor(this),
                                        grib_find_accessor(grib_handle_of_accessor(this), total_length_),
                                        this,
                                        &total_length,
                                        &sec4_length)) != GRIB_SUCCESS) {
        return ret;
    }

    *val = sec4_length;

    return GRIB_SUCCESS;
}
