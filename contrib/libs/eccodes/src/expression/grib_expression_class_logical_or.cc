/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_logical_or.h"

namespace eccodes::expression {


LogicalOr::LogicalOr(grib_context* c, Expression* left, Expression* right)
{
    left_  = left;
    right_ = right;
}

int LogicalOr::evaluate_long(grib_handle* h, long* lres) const
{
    long v1    = 0;
    long v2    = 0;
    double dv1 = 0;
    double dv2 = 0;
    int ret;


    switch (left_->native_type(h)) {
        case GRIB_TYPE_LONG:
            ret = left_->evaluate_long(h, &v1);
            if (ret != GRIB_SUCCESS)
                return ret;
            if (v1 != 0) {
                *lres = 1;
                return ret;
            }
            break;
        case GRIB_TYPE_DOUBLE:
            ret = left_->evaluate_double(h, &dv1);
            if (ret != GRIB_SUCCESS)
                return ret;
            if (dv1 != 0) {
                *lres = 1;
                return ret;
            }
            break;
        default:
            return GRIB_INVALID_TYPE;
    }

    switch (right_->native_type(h)) {
        case GRIB_TYPE_LONG:
            ret = right_->evaluate_long(h, &v2);
            if (ret != GRIB_SUCCESS)
                return ret;
            *lres = v2 ? 1 : 0;
            break;
        case GRIB_TYPE_DOUBLE:
            ret = right_->evaluate_double(h, &dv2);
            if (ret != GRIB_SUCCESS)
                return ret;
            *lres = dv2 ? 1 : 0;
            break;
        default:
            return GRIB_INVALID_TYPE;
    }

    return GRIB_SUCCESS;
}

int LogicalOr::evaluate_double(grib_handle* h, double* dres) const
{
    long lres = 0;
    int ret = evaluate_long(h, &lres);
    *dres = (double)lres;
    return ret;
}

void LogicalOr::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "(");
    left_->print(c, f, out);
    fprintf(out, " || ");
    right_->print(c, f, out);
    fprintf(out, ")");
}

void LogicalOr::destroy(grib_context* c)
{
    left_->destroy(c);
    delete left_;
    right_->destroy(c);
    delete right_;
}

void LogicalOr::add_dependency(grib_accessor* observer)
{
    left_->add_dependency(observer);
    right_->add_dependency(observer);
}

int LogicalOr::native_type(grib_handle* h) const
{
    return GRIB_TYPE_LONG;
}

}  // namespace eccodes::expression

grib_expression* new_logical_or_expression(grib_context* c, grib_expression* left, grib_expression* right) {
    return new eccodes::expression::LogicalOr(c, left, right);
}
