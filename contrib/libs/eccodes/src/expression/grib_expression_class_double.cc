/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_double.h"

namespace eccodes::expression {

int Double::evaluate_long(grib_handle* h, long* lres) const
{
    *lres = value_;
    return GRIB_SUCCESS;
}

int Double::evaluate_double(grib_handle* h, double* dres) const
{
    *dres = value_;
    return GRIB_SUCCESS;
}

void Double::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "double(%g)", value_);
}

Double::Double(grib_context* c, double value)
{
    value_ = value;
}

int Double::native_type(grib_handle* h) const
{
    return GRIB_TYPE_DOUBLE;
}

}  // namespace eccodes::expression

grib_expression* new_double_expression(grib_context* c, double value)
{
    return new eccodes::expression::Double(c, value);
}
