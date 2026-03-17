/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_true.h"

namespace eccodes::expression {

int True::evaluate_long(grib_handle* h, long* lres) const
{
    *lres = 1;
    return GRIB_SUCCESS;
}

int True::evaluate_double(grib_handle* h, double* dres) const
{
    *dres = 1;
    return GRIB_SUCCESS;
}

void True::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "true()");
}

int True::native_type(grib_handle* h) const
{
    return GRIB_TYPE_LONG;
}

}  // namespace eccodes::expression

grib_expression* new_true_expression(grib_context* c) {
    return new eccodes::expression::True();
}

