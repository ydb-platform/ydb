/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_long.h"

namespace eccodes::expression {

int Long::evaluate_long(grib_handle* h, long* lres) const
{
    *lres = value_;
    return GRIB_SUCCESS;
}

int Long::evaluate_double(grib_handle* h, double* dres) const
{
    *dres = value_;
    return GRIB_SUCCESS;
}

void Long::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "long(%ld)", value_);
}

void Long::add_dependency(grib_accessor* observer)
{
    /* grib_expression_long* e = (grib_expression_long*)g; */
}

int Long::native_type(grib_handle* h) const
{
    return GRIB_TYPE_LONG;
}

}  // namespace eccodes::expression

grib_expression* new_long_expression(grib_context* c, long value) {
    return new eccodes::expression::Long(value);
}
