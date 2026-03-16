/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_sub_string.h"

namespace eccodes::expression {

Expression::string SubString::evaluate_string(grib_handle* h, char* buf, size_t* size, int* err) const
{
    *err = 0;
    return value_;
}

void SubString::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "string('%s')", value_);
}

void SubString::destroy(grib_context* c)
{
    grib_context_free_persistent(c, value_);
}

void SubString::add_dependency(grib_accessor* observer)
{
    /* grib_expression_sub_string* e = (grib_expression_sub_string*)g; */
}

SubString::SubString(grib_context* c, const char* value, size_t start, size_t length)
{
    char v[1024] = {0,};

    memcpy(v, value + start, length);
    value_ = grib_context_strdup_persistent(c, v);
}

int SubString::native_type(grib_handle* h) const
{
    return GRIB_TYPE_STRING;
}

}  // namespace eccodes::expression

grib_expression* new_sub_string_expression(grib_context* c, const char* value, size_t start, size_t length)
{
    /* if (start<0) start+=strlen(value);  */
    const size_t slen = strlen(value);

    if (length == 0) {
        grib_context_log(c, GRIB_LOG_ERROR, "Invalid substring: length must be > 0");
        return NULL;
    }
    if (start > slen) { /* to catch a -ve number passed to start */
        grib_context_log(c, GRIB_LOG_ERROR, "Invalid substring: start=%lu", start);
        return NULL;
    }
    if (start + length > slen) {
        grib_context_log(c, GRIB_LOG_ERROR, "Invalid substring: start(=%lu)+length(=%lu) > length('%s'))", start, length, value);
        return NULL;
    }

    return new eccodes::expression::SubString(c, value, start, length);
}
