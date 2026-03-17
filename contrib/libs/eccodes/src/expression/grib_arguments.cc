/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_arguments.h"


grib_arguments* grib_arguments_new(grib_context* c, eccodes::Expression* g, eccodes::Arguments* n)
{
    return new eccodes::Arguments(c, g, n);
}

void grib_arguments_free(grib_context* c, grib_arguments* g)
{
    if (g) {
        // TODO(maee): Check for memory leaks
        // grib_arguments_free(c, g->next_);
        // grib_expression_free(c, g->expression_);
        delete g;
    }
}

namespace eccodes {

Arguments::Arguments(grib_context* c, Expression* g, Arguments* n)
{
    context_    = c;
    expression_ = g;
    next_       = n;
}

Arguments::~Arguments()
{
    grib_arguments_free(context_, next_);
    if (expression_) {
        expression_->destroy(context_);
        delete expression_;
    }
}

void Arguments::print(grib_handle* f) const
{
    if (expression_)
        expression_->print(context_, f, stdout);
    if (next_) {
        printf(",");
        next_->print(f);
    }
}

const char* Arguments::get_name(grib_handle* h, int n) const
{
    const Expression* e = NULL;
    const Arguments* args = this;
    while (args && n-- > 0) {
        args = args->next_;
    }

    if (!args)
        return NULL;

    e = args->expression_;
    return e ? e->get_name() : NULL;
}

const char* Arguments::get_string(grib_handle* h, int n) const
{
    const Arguments* args = this;
    int ret               = 0;
    while (args && n-- > 0) {
        args = args->next_;
    }

    if (!args)
        return NULL;

    const Expression* e = args->expression_;
    return e->evaluate_string(h, NULL, NULL, &ret);
}

long Arguments::get_long(grib_handle* h, int n) const
{
    int ret               = 0;
    long lres             = 0;
    const Arguments* args = this;
    while (args && n-- > 0) {
        args = args->next_;
    }

    if (!args)
        return 0;

    const grib_expression* e = args->expression_;
    ret = e->evaluate_long(h, &lres);
    (void)ret;
    return lres;
}

double Arguments::get_double(grib_handle* h, int n) const
{
    int ret     = 0;
    double dres = 0.0;

    const Arguments* args = this;
    while (args && n-- > 0) {
        args = args->next_;
    }

    if (!args)
        return 0;

    const Expression* e = args->expression_;
    ret = e->evaluate_double(h, &dres);
    (void)ret;
    return dres;
}

grib_expression* Arguments::get_expression(grib_handle* h, int n) const
{
    const Arguments* args = this;
    while (args && n-- > 0) {
        args = args->next_;
    }

    if (!args)
        return 0;

    return args->expression_;
}

int Arguments::get_count() const
{
    const Arguments* args = this;
    int n                 = 0;
    while (args) {
        args = args->next_;
        n++;
    }
    return n;
}

}  // namespace eccodes
