/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_label.h"

grib_accessor_label_t _grib_accessor_label{};
grib_accessor* grib_accessor_label = &_grib_accessor_label;

void grib_accessor_label_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
    length_ = 0;
}

void grib_accessor_label_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_label(this, NULL);
}

long grib_accessor_label_t::get_native_type()
{
    return GRIB_TYPE_LABEL;
}

int grib_accessor_label_t::unpack_string(char* val, size_t* len)
{
    size_t vlen = strlen(name_);
    if (vlen > *len)
        return GRIB_BUFFER_TOO_SMALL;
    *len = vlen;
    strcpy(val, name_);
    return GRIB_SUCCESS;
}
