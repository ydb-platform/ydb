/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_string_compare.h"

namespace eccodes::expression {

/* Note: A fast cut-down version of strcmp which does NOT return -1 */
/* 0 means input strings are equal and 1 means not equal */
GRIB_INLINE static int grib_inline_strcmp(const char* a, const char* b)
{
    if (*a != *b)
        return 1;
    while ((*a != 0 && *b != 0) && *(a) == *(b)) {
        a++;
        b++;
    }
    return (*a == 0 && *b == 0) ? 0 : 1;
}

int StringCompare::evaluate_long(grib_handle* h, long* lres) const
{
    int ret = 0;
    char b1[1024] = {0,};
    size_t l1 = sizeof(b1);
    char b2[1024] = {0,};
    size_t l2 = sizeof(b2);
    const char* v1 = NULL;
    const char* v2 = NULL;


    v1 = left_->evaluate_string(h, b1, &l1, &ret);
    if (!v1 || ret) {
        *lres = 0;
        return ret;
    }

    v2 = right_->evaluate_string(h, b2, &l2, &ret);
    if (!v2 || ret) {
        *lres = 0;
        return ret;
    }

    if (eq_) // IS operator
        *lres = (grib_inline_strcmp(v1, v2) == 0);
    else // ISNOT operator
        *lres = (grib_inline_strcmp(v1, v2) != 0);

    return GRIB_SUCCESS;
}

int StringCompare::evaluate_double(grib_handle* h, double* dres) const
{
    long n;
    int ret = evaluate_long(h, &n);
    *dres   = n;
    return ret;
}

void StringCompare::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "string_compare(");
    left_->print(c, f, out);
    fprintf(out, ",");
    left_->print(c, f, out);
    fprintf(out, ")");
}

void StringCompare::destroy(grib_context* c)
{
    left_->destroy(c);
    delete left_;
    right_->destroy(c);
    delete right_;
}

void StringCompare::add_dependency(grib_accessor* observer)
{
    left_->add_dependency(observer);
    right_->add_dependency(observer);
}

StringCompare::StringCompare(grib_context* c, Expression* left, Expression* right, int eq)
{
    left_  = left;
    right_ = right;
    eq_    = eq;
}

int StringCompare::native_type(grib_handle* h) const
{
    return GRIB_TYPE_LONG;
}

} // namespace eccodes::expression

grib_expression* new_string_compare_expression(grib_context* c, grib_expression* left, grib_expression* right, int eq) {
    return new eccodes::expression::StringCompare(c, left, right, eq);
}
