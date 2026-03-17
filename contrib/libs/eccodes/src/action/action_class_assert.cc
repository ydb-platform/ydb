/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_assert.h"

grib_action* grib_action_create_assert(grib_context* context, grib_expression* expression)
{
    return new eccodes::action::Assert(context, expression);
}

namespace eccodes::action
{

Assert::Assert(grib_context* context, grib_expression* expression)
{
    class_name_ = "action_class_assert";
    next_       = NULL;
    name_       = grib_context_strdup_persistent(context, "assertion");
    op_         = grib_context_strdup_persistent(context, "evaluate");
    context_    = context;
    expression_ = expression;
}

Assert::~Assert()
{
    expression_->destroy(context_);
    delete expression_;
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
}

int Assert::create_accessor(grib_section* p, grib_loader* h)
{
    grib_accessor* as = grib_accessor_factory(p, this, 0, NULL);
    if (!as)
        return GRIB_INTERNAL_ERROR;
    grib_dependency_observe_expression(as, expression_);

    const int err = execute(p->h);
    if (err == GRIB_ASSERTION_FAILURE) {
        grib_context_log(context_, GRIB_LOG_FATAL, "Assert failed");
    }

    grib_push_accessor(as, p->block);

    return GRIB_SUCCESS;
}

//void Assert::dump(FILE* f, int lvl)
//{
    // int i = 0;
    // for (i = 0; i < lvl; i++)
    //     grib_context_print(act->context, f, "     ");
    // grib_expression_print(act->context, self->expression, 0);
    // printf("\n");
//}

int Assert::execute(grib_handle* h)
{
    int ret    = 0;
    double res = 0;

    if ((ret = expression_->evaluate_double(h, &res)) != GRIB_SUCCESS)
        return ret;

    if (res != 0) {
        return GRIB_SUCCESS;
    }
    else {
        grib_context_log(h->context, GRIB_LOG_ERROR, "Assertion failure: ");
        expression_->print(h->context, h, stderr);
        fprintf(stderr, "\n");
        return GRIB_ASSERTION_FAILURE;
    }
}

int Assert::notify_change(grib_accessor* observer, grib_accessor* observed)
{
    int ret = GRIB_SUCCESS;
    long lres;

    if ((ret = expression_->evaluate_long(grib_handle_of_accessor(observed), &lres)) != GRIB_SUCCESS)
        return ret;

    if (lres != 0)
        return GRIB_SUCCESS;
    else
        return GRIB_ASSERTION_FAILURE;
}

}  // namespace eccodes::action
