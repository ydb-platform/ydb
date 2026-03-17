/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_string.h"

namespace eccodes::expression {

String::String(grib_context* c, const char* value) {
    value_ = grib_context_strdup_persistent(c, value);
}

Expression::string String::evaluate_string(grib_handle* h, char* buf, size_t* size, int* err) const
{
    *err = 0;
    return value_;
}

void String::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "string('%s')", value_);
}

void String::destroy(grib_context* c)
{
    grib_context_free_persistent(c, value_);
}

void String::add_dependency(grib_accessor* observer)
{
    /* grib_expression_string* e = (grib_expression_string*)g; */
}

int String::native_type(grib_handle* h) const
{
    return GRIB_TYPE_STRING;
}

} // namespace eccodes::expression

grib_expression* new_string_expression(grib_context* c, const char* value) {
    return new eccodes::expression::String(c, value);
}
