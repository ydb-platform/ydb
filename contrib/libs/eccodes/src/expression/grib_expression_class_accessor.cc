/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_accessor.h"

namespace eccodes::expression {

const char* Accessor::get_name() const
{
    return name_;
}

int Accessor::evaluate_long(grib_handle* h, long* result) const
{
    return grib_get_long_internal(h, name_, result);
}

int Accessor::evaluate_double(grib_handle* h, double* result) const
{
    return grib_get_double_internal(h, name_, result);
}

Accessor::string Accessor::evaluate_string(grib_handle* h, char* buf, size_t* size, int* err) const
{
    char mybuf[1024] = {0,};
    long start = start_;
    if (length_ > sizeof(mybuf)) {
        *err = GRIB_INVALID_ARGUMENT;
        return NULL;
    }

    if (!buf) {
        *err = GRIB_INVALID_ARGUMENT;
        return NULL;
    }
    if ((*err = grib_get_string_internal(h, name_, mybuf, size)) != GRIB_SUCCESS)
        return NULL;

    if (start_ < 0)
        start += *size;

    if (length_ != 0) {
        if (start >= 0)
            memcpy(buf, mybuf + start, length_);
        buf[length_] = 0;
    }
    else {
        memcpy(buf, mybuf, *size);
        if (*size == 1024)
            *size = *size - 1; /* ECC-336 */
        buf[*size] = 0;
    }
    return buf;
}

void Accessor::print(grib_context* c, grib_handle* hand, FILE* out) const
{
    int err = 0;
    fprintf(out, "access('%s", name_);
    if (hand) {
        const int ntype = native_type(hand);
        if (ntype == GRIB_TYPE_STRING) {
            char buf[256] = {0,};
            size_t len = sizeof(buf);
            err = grib_get_string(hand, name_, buf, &len);
            if (!err) fprintf(out, "=%s", buf);
        }
        else if (ntype == GRIB_TYPE_LONG) {
            long lVal = 0;
            err = grib_get_long(hand, name_, &lVal);
            if (!err) fprintf(out, "=%ld", lVal);
        }
    }
    fprintf(out, "')");
}

void Accessor::destroy(grib_context* c)
{
    grib_context_free_persistent(c, name_);
}

void Accessor::add_dependency(grib_accessor* observer)
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

Accessor::Accessor(grib_context* c, const char* name, long start, size_t length)
{
    name_    = grib_context_strdup_persistent(c, name);
    start_   = start;
    length_  = length;
}

int Accessor::native_type(grib_handle* h) const
{
    int type = 0;
    int err;
    if ((err = grib_get_native_type(h, name_, &type)) != GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Error in evaluating the type of '%s': %s", name_, grib_get_error_message(err));
    }
    return type;
}

}  // namespace eccodes::expression

grib_expression* new_accessor_expression(grib_context* c, const char* name, long start, size_t length) {
    return new eccodes::expression::Accessor(c, name, start, length);
}
