/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_iterator.h"

grib_accessor_iterator_t _grib_accessor_iterator{};
grib_accessor* grib_accessor_iterator = &_grib_accessor_iterator;

void grib_accessor_iterator_t::init(const long l, grib_arguments* args)
{
    grib_accessor_gen_t::init(l, args);
    args_ = args;
}

void grib_accessor_iterator_t::dump(eccodes::Dumper* dumper)
{
    /* TODO: pass args */
    dumper->dump_label(this, NULL);
}
