/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_if.h"


grib_action* grib_action_create_if(grib_context* context,
                                   grib_expression* expression,
                                   grib_action* block_true, grib_action* block_false, int transient,
                                   int lineno, const char* file_being_parsed)
{
    return new eccodes::action::If(context, expression, block_true, block_false, transient, lineno, file_being_parsed);  
}

namespace eccodes::action
{

If::If(grib_context* context,
       grib_expression* expression,
       grib_action* block_true, grib_action* block_false, int transient,
       int lineno, const char* file_being_parsed)
{
    char name[1024];
    const size_t nameLen = sizeof(name);

    class_name_  = "action_class_if";
    op_          = grib_context_strdup_persistent(context, "section");
    context_     = context;
    expression_  = expression;
    block_true_  = block_true;
    block_false_ = block_false;
    transient_   = transient;

    if (transient)
        snprintf(name, nameLen, "__if%p", (void*)this);
    else
        snprintf(name, nameLen, "_if%p", (void*)this);

    name_       = grib_context_strdup_persistent(context, name);
    debug_info_ = NULL;
    if (context->debug > 0 && file_being_parsed) {
        /* Construct debug information showing definition file and line */
        /* number of IF statement */
        char debug_info[1024];
        const size_t infoLen = sizeof(debug_info);
        snprintf(debug_info, infoLen, "File=%s line=%d", file_being_parsed, lineno);
        debug_info_ = grib_context_strdup_persistent(context, debug_info);
    }
}

If::~If()
{
    Action* t = block_true_;
    Action* f = block_false_;

    while (t) {
        Action* nt = t->next_;
        delete t;
        t = nt;
    }

    while (f) {
        Action* nf = f->next_;
        delete f;
        f = nf;
    }

    expression_->destroy(context_);
    delete expression_;

    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, debug_info_);
    grib_context_free_persistent(context_, op_);
}

int If::create_accessor(grib_section* p, grib_loader* h)
{
    grib_action* next = NULL;
    int ret           = GRIB_SUCCESS;
    long lres         = 0;

    grib_accessor* as = NULL;
    grib_section* gs  = NULL;

    as = grib_accessor_factory(p, this, 0, NULL);
    if (!as)
        return GRIB_INTERNAL_ERROR;
    gs = as->sub_section_;
    grib_push_accessor(as, p->block);

    if ((ret = expression_->evaluate_long(p->h, &lres)) != GRIB_SUCCESS)
        return ret;

    if (lres)
        next = block_true_;
    else
        next = block_false_;

    if (p->h->context->debug > 1) {
        fprintf(stderr, "EVALUATE create_accessor_handle ");
        expression_->print(p->h->context, p->h, stderr);
        fprintf(stderr, " [%s][_if%p]\n", (next == block_true_ ? "true" : "false"), (void*)this);

        /*grib_dump_action_branch(stdout,next,5);*/
    }

    gs->branch = next;
    grib_dependency_observe_expression(as, expression_);

    while (next) {
        ret = next->create_accessor(gs, h);
        if (ret != GRIB_SUCCESS)
            return ret;
        next = next->next_;
    }

    return GRIB_SUCCESS;
}

static void print_expression_debug_info(grib_context* ctx, grib_expression* exp, grib_handle* h)
{
    exp->print(ctx, h, stderr); /* writes without a newline */
    fprintf(stderr, "\n");
}

int If::execute(grib_handle* h)
{
    grib_action* next = NULL;
    grib_context* ctx = h->context;
    int ret           = 0;
    long lres         = 0;

    /* See GRIB-394 */
    int type = expression_->native_type(h);
    if (type != GRIB_TYPE_DOUBLE) {
        if ((ret = expression_->evaluate_long(h, &lres)) != GRIB_SUCCESS) {
            if (ret == GRIB_NOT_FOUND)
                lres = 0;
            else {
                if (ctx->debug)
                    print_expression_debug_info(ctx, expression_, h);
                return ret;
            }
        }
    }
    else {
        double dres = 0.0;
        ret         = expression_->evaluate_double(h, &dres);
        lres        = (long)dres;
        if (ret != GRIB_SUCCESS) {
            if (ret == GRIB_NOT_FOUND)
                lres = 0;
            else {
                if (ctx->debug)
                    print_expression_debug_info(ctx, expression_, h);
                return ret;
            }
        }
    }

    if (lres)
        next = block_true_;
    else
        next = block_false_;

    while (next) {
        ret = next->execute(h);
        if (ret != GRIB_SUCCESS)
            return ret;
        next = next->next_;
    }

    return GRIB_SUCCESS;
}

void If::dump(FILE* f, int lvl)
{
    int i = 0;

    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");

    printf("if(%s) { ", name_);
    expression_->print(context_, 0, stdout);
    printf("\n");

    if (block_true_) {
        /*      grib_context_print(context_,f,"IF \t TODO \n");  TODO */
        grib_dump_action_branch(f, block_true_, lvl + 1);
    }
    if (block_false_) {
        printf("}\n");
        for (i = 0; i < lvl; i++)
            grib_context_print(context_, f, "     ");
        printf("else(%s) { ", name_);
        expression_->print(context_, 0, stdout);
        /*     grib_context_print(context_,f,"ELSE \n" );*/
        grib_dump_action_branch(f, block_false_, lvl + 1);
    }
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    printf("}\n");
}

grib_action* If::reparse(grib_accessor* acc, int* doit)
{
    int ret   = 0;
    long lres = 0;

    /* printf("reparse %s %s\n",name_,acc->name); */

    if ((ret = expression_->evaluate_long(grib_handle_of_accessor(acc), &lres)) != GRIB_SUCCESS)
        grib_context_log(acc->context_,
                         GRIB_LOG_ERROR, "action_class_if::reparse: grib_expression_evaluate_long failed: %s",
                         grib_get_error_message(ret));

    if (lres)
        return block_true_;
    else
        return block_false_;
}

}  // namespace eccodes::action
