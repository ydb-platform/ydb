/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_binop.h"

namespace eccodes::expression {

int Binop::evaluate_long(grib_handle* h, long* lres) const
{
    long v1 = 0;
    long v2 = 0;

//     {
//         int typeLeft, typeRight;
//         const char* nameLeft;
//         const char* nameRight;
//         typeLeft = grib_expression_native_type(h, left_);
//         typeRight = grib_expression_native_type(h, right_);
//         nameLeft = grib_expression_get_name(left_);
//         nameRight= grib_expression_get_name(right_);
//         printf("eval_long nameLeft=%s (type=%d), nameRight=%s (type=%d)\n",nameLeft,typeLeft, nameRight,typeRight);
//         grib_expression_print(h->context, g, h);
//         printf("\n");
//     }

    int ret = left_->evaluate_long(h, &v1);
    if (ret != GRIB_SUCCESS)
        return ret;

    ret = right_->evaluate_long(h, &v2);
    if (ret != GRIB_SUCCESS)
        return ret;

    *lres = long_func_(v1, v2);
    return GRIB_SUCCESS;
}

int Binop::evaluate_double(grib_handle* h, double* dres) const
{
    double v1 = 0.0;
    double v2 = 0.0;

//     {
//         int typeLeft, typeRight;
//         const char* nameLeft;
//         const char* nameRight;
//         typeLeft = grib_expression_native_type(h, left_);
//         typeRight = grib_expression_native_type(h, right_);
//         nameLeft = grib_expression_get_name(left_);
//         nameRight= grib_expression_get_name(right_);
//         printf("eval_dbl nameLeft=%s (type=%d), nameRight=%s (type=%d)\n",nameLeft,typeLeft, nameRight,typeRight);
//         grib_expression_print(h->context, g, h);
//         printf("\n");
//     }

    int ret = left_->evaluate_double(h, &v1);
    if (ret != GRIB_SUCCESS)
        return ret;

    ret = right_->evaluate_double(h, &v2);
    if (ret != GRIB_SUCCESS)
        return ret;

    *dres = double_func_ ? double_func_(v1, v2) : long_func_(v1, v2);

    return GRIB_SUCCESS;
}

void Binop::print(grib_context* c, grib_handle* f, FILE* out) const
{
    // Cover a subset of the most commonly used functions
    // TODO(masn): Can be done in a much better way!
    //             e.g., the yacc parser passing in the functions name
    if (long_func_ && long_func_.target<std::equal_to<long>>()) {
        fprintf(out, "equals(");
    } else if (long_func_ && long_func_.target<std::not_equal_to<long>>()) {
        fprintf(out, "not_equals(");
    } else if (long_func_ && long_func_.target<std::less<long>>()) {
        fprintf(out, "less_than(");
    } else if (long_func_ && long_func_.target<std::greater<long>>()) {
        fprintf(out, "greater_than(");
    } else {
        fprintf(out, "binop(");
    }
    left_->print(c, f, out);
    fprintf(out, ",");
    right_->print(c, f, out);
    fprintf(out, ")");
}

void Binop::destroy(grib_context* c)
{
    left_->destroy(c);
    delete left_;
    right_->destroy(c);
    delete right_;
}

void Binop::add_dependency(grib_accessor* observer)
{
    left_->add_dependency(observer);
    right_->add_dependency(observer);
}

Binop::Binop(grib_context* c,
             BinopLongProc long_func,
             BinopDoubleProc double_func,
             Expression* left, Expression* right)
{
    left_        = left;
    right_       = right;
    long_func_   = long_func;
    double_func_ = double_func;
}

int Binop::native_type(grib_handle* h) const
{
    /* See GRIB-394 : The type of this binary expression will be double if any of its operands are double */
    if (left_->native_type(h) == GRIB_TYPE_DOUBLE ||
        right_->native_type(h) == GRIB_TYPE_DOUBLE) {
        return GRIB_TYPE_DOUBLE;
    }
    return long_func_ ? GRIB_TYPE_LONG : GRIB_TYPE_DOUBLE;
}

}  // namespace eccodes::expression


grib_expression* new_binop_expression(grib_context* c, grib_binop_long_proc long_func, grib_binop_double_proc double_func, grib_expression* left, grib_expression* right) {
    return new eccodes::expression::Binop(c, long_func, double_func, left, right);
}

//grib_expression* new_binop_expression(grib_context* c,
//                                      eccodes::Expression::BinopLongProc long_func,
//                                      eccodes::Expression::BinopDoubleProc double_func,
//                                      grib_expression* left, grib_expression* right)
//{
//    return new eccodes::expression::Binop(c, long_func, double_func, left, right);
//}
