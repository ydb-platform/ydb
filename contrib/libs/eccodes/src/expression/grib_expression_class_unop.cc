/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_unop.h"

namespace eccodes::expression {

Unop::Unop(grib_context* c, Expression::UnopLongProc long_func,
           Expression::UnopDoubleProc double_func,
           Expression* exp) : exp_(exp), long_func_(long_func), double_func_(double_func) {}

int Unop::evaluate_long(grib_handle* h, long* lres) const
{
    long v = 0;
    int ret = exp_->evaluate_long(h, &v);
    if (ret != GRIB_SUCCESS)
        return ret;
    *lres = long_func_(v);
    return GRIB_SUCCESS;
}

int Unop::evaluate_double(grib_handle* h, double* dres) const
{
    double v = 0;
    int ret = exp_->evaluate_double(h, &v);
    if (ret != GRIB_SUCCESS)
        return ret;
    *dres = double_func_ ? double_func_(v) : long_func_(v);
    return GRIB_SUCCESS;
}

Expression::string Unop::get_name() const
{
    return exp_->get_name();
}

void Unop::print(grib_context* c, grib_handle* f, FILE* out) const
{
    fprintf(out, "unop(");
    exp_->print(c, f, out);
    fprintf(out, ")");
}

void Unop::destroy(grib_context* c)
{
    exp_->destroy(c);
    delete exp_;
}

void Unop::add_dependency(grib_accessor* observer)
{
    exp_->add_dependency(observer);
}

int Unop::native_type(grib_handle* h) const
{
    return long_func_ ? GRIB_TYPE_LONG : GRIB_TYPE_DOUBLE;
}

}  // namespace eccodes::expression

grib_expression* new_unop_expression(grib_context* c, grib_unop_long_proc long_func, grib_unop_double_proc double_func, grib_expression* exp) {
    return new eccodes::expression::Unop(c, long_func, double_func, exp);
}
