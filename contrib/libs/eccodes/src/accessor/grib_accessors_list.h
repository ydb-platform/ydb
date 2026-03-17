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

#include "grib_accessor.h"

class grib_accessors_list
{
public:
    grib_accessors_list();
    ~grib_accessors_list();

    int value_count(size_t* count);
    void push(grib_accessor* a, int rank);
    grib_accessors_list* last();
    int unpack_long(long* val, size_t* buffer_len);
    int unpack_double(double* val, size_t* buffer_len);
    int unpack_float(float* val, size_t* buffer_len);
    int unpack_string(char** val, size_t* buffer_len);

    int rank() const { return rank_; }

    grib_accessor* accessor    = nullptr;
    grib_accessors_list* next_ = nullptr;
    grib_accessors_list* prev_ = nullptr;
    grib_accessors_list* last_ = nullptr;

private:
    int rank_ = 0;
};

grib_accessors_list* grib_accessors_list_create(grib_context* c);
void grib_accessors_list_delete(grib_context* c, grib_accessors_list* al);
