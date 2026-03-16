/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_section_length.h"

grib_accessor_section_length_t _grib_accessor_section_length{};
grib_accessor* grib_accessor_section_length = &_grib_accessor_section_length;

void grib_accessor_section_length_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_unsigned_t::init(len, arg);
    parent_->aclength = this;
    length_           = len;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
    ECCODES_ASSERT(length_ >= 0);
}

void grib_accessor_section_length_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_long(this, NULL);
}

int grib_accessor_section_length_t::value_count(long* c)
{
    *c = 1;
    return 0;
}
