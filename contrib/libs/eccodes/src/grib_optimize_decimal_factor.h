/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#ifndef GRIB_OPTIMIZE_DECIMAL_FACTOR_H
#define GRIB_OPTIMIZE_DECIMAL_FACTOR_H

#include "grib_api_internal.h"

int grib_optimize_decimal_factor(grib_accessor* a, const char* reference_value,
                                 const double pmax, const double pmin, const int knbit,
                                 const int compat_gribex, const int compat_32bit,
                                 long* kdec, long* kbin, double* ref);

#endif
