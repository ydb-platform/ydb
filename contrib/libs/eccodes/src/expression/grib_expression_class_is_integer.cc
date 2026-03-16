/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_is_integer.h"

namespace eccodes::expression {

Expression::string IsInteger::get_name() const
{
    return name_;
}

int IsInteger::evaluate_long(grib_handle* h, long* result) const
{
    int err = 0;
    char mybuf[1024] = {0,};
    size_t size = 1024;
    char* p     = 0;
    char* start = 0;

    if ((err = grib_get_string_internal(h, name_, mybuf, &size)) != GRIB_SUCCESS)
        return err;

    start = mybuf + start_;

    if (length_ > 0)
        start[length_] = 0;

    strtol(start, &p, 10);

    if (*p != 0)
        *result = 0;
    else
        *result = 1;

    return err;
}

int IsInteger::evaluate_double(grib_handle* h, double* result) const
{
    int err = 0;
    long lresult = 0;

    err = evaluate_long(h, &lresult);
    *result = lresult;
    return err;
}

Expression::string IsInteger::evaluate_string(grib_handle* h, char* buf, size_t* size, int* err) const
{
    long lresult   = 0;
    double dresult = 0.0;

    switch (native_type(h)) {
        case GRIB_TYPE_LONG:
            *err = evaluate_long(h, &lresult);
            snprintf(buf, 32, "%ld", lresult);
            break;
        case GRIB_TYPE_DOUBLE:
            *err = evaluate_double(h, &dresult);
            snprintf(buf, 32, "%g", dresult);
            break;
    }
    return buf;
}

void IsInteger::print(grib_context* c, grib_handle* f, FILE* out) const
{
    // grib_expression_is_integer* e = (grib_expression_is_integer*)g;
    // printf("access('%s", name_);
    // if (f) {
    //     long s = 0;
    //     grib_get_long(f, name_, &s);
    //     printf("=%ld", s);
    // }
    // printf("')");
}

void IsInteger::destroy(grib_context* c)
{
    grib_context_free_persistent(c, name_);
}

void IsInteger::add_dependency(grib_accessor* observer)
{
    grib_accessor* observed       = grib_find_accessor(grib_handle_of_accessor(observer), name_);

    if (!observed) {
        /* grib_context_log(observer->context, GRIB_LOG_ERROR, */
        /* "Error in accessor_add_dependency: cannot find [%s]", name_); */
        /* ECCODES_ASSERT(observed); */
        return;
    }

    grib_dependency_add(observer, observed);
}

IsInteger::IsInteger(grib_context* c, const char* name, int start, int length)
{
    name_     = grib_context_strdup_persistent(c, name);
    start_    = start;
    length_   = length;
}

int IsInteger::native_type(grib_handle* h) const
{
    return GRIB_TYPE_LONG;
}

}  // namespace eccodes::expression

grib_expression* new_is_integer_expression(grib_context* c, const char* name, int start, int length) {
    return new eccodes::expression::IsInteger(c, name, start, length);
}
