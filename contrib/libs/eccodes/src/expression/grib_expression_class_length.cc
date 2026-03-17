/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_length.h"

namespace eccodes::expression {

Expression::string Length::get_name() const
{
    return name_;
}

int Length::evaluate_long(grib_handle* h, long* result) const
{
    int err = 0;
    char mybuf[1024] = {0,};
    size_t size = 1024;
    if ((err = grib_get_string_internal(h, name_, mybuf, &size)) != GRIB_SUCCESS)
        return err;

    *result = strlen(mybuf);
    return err;
}

int Length::evaluate_double(grib_handle* h, double* result) const
{
    char mybuf[1024] = {0,};
    size_t size = 1024;
    int err = 0;
    if ((err = grib_get_string_internal(h, name_, mybuf, &size)) != GRIB_SUCCESS)
        return err;

    *result = strlen(mybuf);
    return err;
}

Expression::string Length::evaluate_string(grib_handle* h, char* buf, size_t* size, int* err) const
{
    char mybuf[1024] = {0,};
    ECCODES_ASSERT(buf);
    if ((*err = grib_get_string_internal(h, name_, mybuf, size)) != GRIB_SUCCESS)
        return NULL;

    snprintf(buf, 32, "%ld", (long)strlen(mybuf));
    return buf;
}

void Length::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "access('%s", name_);
    if (f) {
        long s = 0;
        grib_get_long(f, name_, &s);
        fprintf(out, "=%ld", s);
    }
    fprintf(out, "')");
}

void Length::destroy(grib_context* c)
{
    grib_context_free_persistent(c, name_);
}

void Length::add_dependency(grib_accessor* observer)
{
    grib_accessor* observed = grib_find_accessor(grib_handle_of_accessor(observer), name_);

    if (!observed) {
        /* grib_context_log(observer->context, GRIB_LOG_ERROR, */
        /* "Error in accessor_add_dependency: cannot find [%s]", name_); */
        /* ECCODES_ASSERT(observed); */
        return;
    }

    grib_dependency_add(observer, observed);
}

Length::Length(grib_context* c, const char* name)
{
    name_ = grib_context_strdup_persistent(c, name);
}

int Length::native_type(grib_handle* h) const
{
    return GRIB_TYPE_LONG;
}

} // namespace eccodes::expression

grib_expression* new_length_expression(grib_context* c, const char* name) {
    return new eccodes::expression::Length(c, name);
}
