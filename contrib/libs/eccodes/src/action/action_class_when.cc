/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_when.h"


/* The check on self->loop can only be done in non-threaded mode */
#if defined(DEBUG) && GRIB_PTHREADS == 0 && GRIB_OMP_THREADS == 0
    #define CHECK_LOOP 1
#endif

grib_action* grib_action_create_when(grib_context* context,
                                     grib_expression* expression,
                                     grib_action* block_true, grib_action* block_false)
{
    return new eccodes::action::When(context, expression, block_true, block_false);
}

namespace eccodes::action
{

When::When(grib_context* context, grib_expression* expression, grib_action* block_true, grib_action* block_false)
{
    char name[1024];
    const size_t nameLen = sizeof(name);

    class_name_  = "action_class_when";
    op_          = grib_context_strdup_persistent(context, "when");
    context_     = context;
    expression_  = expression;
    block_true_  = block_true;
    block_false_ = block_false;

    snprintf(name, nameLen, "_when%p", (void*)expression);

    debug_info_ = NULL;
    if (context->debug > 0) {
        /* Construct debug information showing definition file and line */
        /* number of IF statement */
        const char* fbp = file_being_parsed();
        if (fbp) {
            char debug_info[1024];
            const size_t infoLen = sizeof(debug_info);
            snprintf(debug_info, infoLen, "File=%s", fbp);
            debug_info_ = grib_context_strdup_persistent(context, debug_info);
        }
    }

    name_ = grib_context_strdup_persistent(context, name);
}

When::~When()
{
    grib_action* t = block_true_;

    while (t) {
        grib_action* nt = t->next_;
        delete t;
        t = nt;
    }

    t = block_false_;
    while (t) {
        grib_action* nt = t->next_;
        delete t;
        t = nt;
    }

    expression_->destroy(context_);
    delete expression_;

    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, debug_info_);
    grib_context_free_persistent(context_, op_);
}

int When::create_accessor(grib_section* p, grib_loader* h)
{
    grib_accessor* as = grib_accessor_factory(p, this, 0, 0);
    if (!as)
        return GRIB_INTERNAL_ERROR;

    grib_dependency_observe_expression(as, expression_);

    grib_push_accessor(as, p->block);

    return GRIB_SUCCESS;
}

void When::dump(FILE* f, int lvl)
{
    int i = 0;

    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");

    printf("when(%s) { ", name_);
    expression_->print(context_, 0, stdout);
    printf("\n");

    grib_dump_action_branch(f, block_true_, lvl + 1);

    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    printf("}");

    if (block_false_) {
        printf(" else { ");

        grib_dump_action_branch(f, block_true_, lvl + 1);

        for (i = 0; i < lvl; i++)
            grib_context_print(context_, f, "     ");
        printf("}");
    }
    printf("\n");
}

#ifdef CHECK_LOOP
    #define SET_LOOP(v) loop_ = v;
#else
    #define SET_LOOP(v)
#endif

int When::notify_change(grib_accessor* observer, grib_accessor* observed)
{
    grib_action* b = NULL;
    int ret        = GRIB_SUCCESS;
    long lres;

    /* ECC-974: observed->parent will change as a result of the execute
     * so must store the handle once here (in 'hand') rather than call
     * grib_handle_of_accessor(observed) later
     */
    grib_handle* hand = grib_handle_of_accessor(observed);

    if ((ret = expression_->evaluate_long(hand, &lres)) != GRIB_SUCCESS)
        return ret;
#ifdef CHECK_LOOP
    if (loop_) {
        printf("LOOP detected...\n");
        printf("WHEN triggered by %s %ld\n", observed->name_, lres);
        expression_->print(observed->context_, 0, stderr);
        fprintf(stderr, "\n");
        return ret;
    }
#endif
    SET_LOOP(1);

    if (hand->context->debug > 0) {
        grib_context_log(hand->context, GRIB_LOG_DEBUG,
                         "------------- SECTION action %s is triggered by [%s] (%s)",
                         name_, observed->name_, debug_info_ ? debug_info_ : "no debug info");
        expression_->print(observed->context_, 0, stderr);
        fprintf(stderr, "\n");
    }

    if (lres)
        b = block_true_;
    else
        b = block_false_;

    while (b) {
        ret = b->execute(hand);
        if (ret != GRIB_SUCCESS) {
            SET_LOOP(0);
            return ret;
        }
        b = b->next_;
    }

    SET_LOOP(0);

    return GRIB_SUCCESS;
}

}  // namespace eccodes::action
