/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression.h"

namespace eccodes {

int Expression::native_type(grib_handle* h) const
{
    grib_context_log(h->context, GRIB_LOG_FATAL, "%s: No native_type() in %s", __func__, class_name());
    return 0;
}

int Expression::evaluate_long(grib_handle* h, long* result) const
{
    return GRIB_INVALID_TYPE;
}

int Expression::evaluate_double(grib_handle* h, double* result) const
{
    return GRIB_INVALID_TYPE;
}

const char* Expression::evaluate_string(grib_handle* h, char* buf, size_t* size, int* err) const
{
    grib_context_log(h->context, GRIB_LOG_ERROR, "%s: No evaluate_string() in %s", __func__, class_name());
    *err = GRIB_INVALID_TYPE;

    return nullptr;
}

const char* Expression::get_name() const
{
    grib_context_log(grib_context_get_default(), GRIB_LOG_FATAL, "%s: No get_name() in %s", __func__, class_name());
    return nullptr;
}

void Expression::print(grib_context* ctx, grib_handle* f, FILE* out) const {}

void Expression::add_dependency(grib_accessor* observer) {}

}  // namespace eccodes
