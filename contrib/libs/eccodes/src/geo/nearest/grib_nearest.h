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

namespace eccodes::geo_nearest {

class Nearest {
public:
    virtual ~Nearest() {}
    virtual int init(grib_handle*, grib_arguments*) = 0;
    virtual int find(grib_handle*, double, double, unsigned long, double*, double*, double*, double*, int*, size_t*) = 0;
    virtual int destroy() = 0;
    virtual Nearest* create() = 0;

protected:
    int grib_nearest_find_generic(grib_handle*, double, double, unsigned long,
                                  const char*,
                                  double**,
                                  int*,
                                  double**,
                                  int*,
                                  double**,
                                  double*, double*, double*, double*, int*, size_t*);

    grib_handle* h_ = nullptr;
    double* values_ = nullptr;
    size_t values_count_ = 0;
    unsigned long flags_ = 0;
    const char* class_name_ = nullptr;
};

Nearest* gribNearestNew(const grib_handle*, int*);
int gribNearestDelete(Nearest*);

}  // namespace eccodes::geo_nearest

int grib_nearest_get_radius(grib_handle* h, double* radiusInKm);
void grib_binary_search(const double xx[], const size_t n, double x, size_t* ju, size_t* jl);
