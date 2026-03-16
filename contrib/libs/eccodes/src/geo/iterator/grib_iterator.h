/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include "grib_api_internal.h"

// GRIB geoiterator, class supporting geographic iteration of values on a GRIB message

namespace eccodes::geo_iterator {

class Iterator
{
public:
    virtual ~Iterator() {}
    virtual int init(grib_handle*, grib_arguments*)       = 0;
    virtual int next(double*, double*, double*) const     = 0;
    virtual int previous(double*, double*, double*) const = 0;
    virtual int reset()                                   = 0;
    virtual int destroy()                                 = 0;
    virtual bool has_next() const                         = 0;
    virtual Iterator* create() const                      = 0;

    unsigned long flags_ = 0;

protected:
    grib_handle* h_ = nullptr;
    double* data_ = nullptr;   // data values
    mutable long e_ = 0;       // current element
    size_t nv_ = 0;            // number of values
    const char* class_name_ = nullptr;
};

eccodes::geo_iterator::Iterator* gribIteratorNew(const grib_handle*, unsigned long, int*);
int gribIteratorDelete(eccodes::geo_iterator::Iterator*);

}  // namespace eccodes::geo_iterator
