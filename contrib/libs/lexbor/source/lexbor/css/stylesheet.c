/*
 * Copyright (C) 2021-2022 Alexander Borisov
 *
 * Author: Alexander Borisov <borisov@lexbor.com>
 */

#include "lexbor/css/css.h"
#include "lexbor/css/stylesheet.h"
#include "lexbor/css/parser.h"
#include "lexbor/css/at_rule.h"
#include "lexbor/css/property.h"
#include "lexbor/css/rule.h"
#include "lexbor/css/state.h"
#include "lexbor/css/selectors/selectors.h"
#include "lexbor/css/selectors/state.h"


static bool
lxb_css_stylesheet_list_rules_state(lxb_css_parser_t *parser,
                                    const lxb_css_syntax_token_t *token, void *ctx);

static bool
lxb_css_stylesheet_list_rules_next(lxb_css_parser_t *parser,
                                   const lxb_css_syntax_token_t *token, void *ctx);

static lxb_status_t
lxb_css_stylesheet_list_rules_end(lxb_css_parser_t *parser,
                                  const lxb_css_syntax_token_t *token,
                                  void *ctx, bool failed);

static bool
lxb_css_stylesheet_at_rule_state(lxb_css_parser_t *parser,
                                 const lxb_css_syntax_token_t *token, void *ctx);

static bool
lxb_css_stylesheet_at_rule_block(lxb_css_parser_t *parser,
                                 const lxb_css_syntax_token_t *token, void *ctx);

static lxb_status_t
lxb_css_stylesheet_at_rule_end(lxb_css_parser_t *parser,
                               const lxb_css_syntax_token_t *token,
                               void *ctx, bool failed);

static bool
lxb_css_stylesheet_qualified_rule_state(lxb_css_parser_t *parser,
                                        const lxb_css_syntax_token_t *token, void *ctx);

static bool
lxb_css_stylesheet_qualified_rule_block(lxb_css_parser_t *parser,
                                        const lxb_css_syntax_token_t *token, void *ctx);

static bool
lxb_css_stylesheet_qualified_rule_back(lxb_css_parser_t *parser,
                                       const lxb_css_syntax_token_t *token, void *ctx);

static lxb_status_t
lxb_css_stylesheet_qualified_rule_end(lxb_css_parser_t *parser,
                                      const lxb_css_syntax_token_t *token,
                                      void *ctx, bool failed);

static bool
lxb_css_stylesheet_declarations_name(lxb_css_parser_t *parser,
                                     const lxb_css_syntax_token_t *token, void *ctx);

static bool
lxb_css_stylesheet_declarations_value(lxb_css_parser_t *parser,
                                      const lxb_css_syntax_token_t *token, void *ctx);

static lxb_status_t
lxb_css_stylesheet_declaration_end(lxb_css_parser_t *parser, void *ctx,
                                   bool important, bool failed);

static lxb_status_t
lxb_css_stylesheet_declarations_end(lxb_css_parser_t *parser,
                                    const lxb_css_syntax_token_t *token,
                                    void *ctx, bool failed);

static bool
lxb_css_stylesheet_declarations_at_rule_state(lxb_css_parser_t *parser,
                                              const lxb_css_syntax_token_t *token, void *ctx);

static bool
lxb_css_stylesheet_declarations_at_rule_block(lxb_css_parser_t *parser,
                                              const lxb_css_syntax_token_t *token, void *ctx);

static lxb_status_t
lxb_css_stylesheet_declarations_at_rule_end(lxb_css_parser_t *parser,
                                            const lxb_css_syntax_token_t *token,
                                            void *ctx, bool failed);

static bool
lxb_css_stylesheet_declarations_bad(lxb_css_parser_t *parser,
                                    const lxb_css_syntax_token_t *token, void *ctx);


static const lxb_css_syntax_cb_at_rule_t lxb_css_stylesheet_at_rule = {
    .state = lxb_css_stylesheet_at_rule_state,
    .block = lxb_css_stylesheet_at_rule_block,
    .failed = lxb_css_state_failed,
    .end = lxb_css_stylesheet_at_rule_end
};

static const lxb_css_syntax_cb_qualified_rule_t lxb_css_stylesheet_qualified_rule = {
    .state = lxb_css_stylesheet_qualified_rule_state,
    .block = lxb_css_stylesheet_qualified_rule_block,
    .failed = lxb_css_state_failed,
    .end = lxb_css_stylesheet_qualified_rule_end
};

static const lxb_css_syntax_cb_list_rules_t lxb_css_stylesheet_list_rules = {
    .cb.state = lxb_css_stylesheet_list_rules_state,
    .cb.failed = lxb_css_state_failed,
    .cb.end = lxb_css_stylesheet_list_rules_end,
    .next = lxb_css_stylesheet_list_rules_next,
    .at_rule = &lxb_css_stylesheet_at_rule,
    .qualified_rule = &lxb_css_stylesheet_qualified_rule
};

static const lxb_css_syntax_cb_at_rule_t lxb_css_stylesheet_declarations_at_rule = {
    .state = lxb_css_stylesheet_declarations_at_rule_state,
    .block = lxb_css_stylesheet_declarations_at_rule_block,
    .failed = lxb_css_state_failed,
    .end = lxb_css_stylesheet_declarations_at_rule_end
};

static const lxb_css_syntax_cb_declarations_t lxb_css_stylesheet_declarations = {
    .cb.state = lxb_css_stylesheet_declarations_name,
    .cb.block = lxb_css_stylesheet_declarations_value,
    .cb.failed = lxb_css_stylesheet_declarations_bad,
    .cb.end = lxb_css_stylesheet_declarations_end,
    .declaration_end = lxb_css_stylesheet_declaration_end,
    .at_rule = &lxb_css_stylesheet_declarations_at_rule
};


lxb_css_stylesheet_t *
lxb_css_stylesheet_create(lxb_css_memory_t *memory)
{
    return (lxb_css_stylesheet_t *) lexbor_mraw_calloc(memory->mraw,
                                                       sizeof(lxb_css_stylesheet_t));
}

lxb_css_stylesheet_t *
lxb_css_stylesheet_destroy(lxb_css_stylesheet_t *sst, bool destroy_memory)
{
    lxb_css_memory_t *memory;

    if (sst == NULL) {
        return NULL;
    }

    memory = sst->memory;

    (void) lexbor_mraw_free(memory->mraw, sst);

    if (destroy_memory) {
        (void) lxb_css_memory_destroy(memory, true);
    }

    return NULL;
}

lxb_status_t
lxb_css_stylesheet_prepare(lxb_css_parser_t *parser, lxb_css_memory_t *memory,
                           lxb_css_selectors_t *selectors)
{
    if (parser->stage != LXB_CSS_PARSER_CLEAN) {
        if (parser->stage == LXB_CSS_PARSER_RUN) {
            return LXB_STATUS_ERROR_WRONG_ARGS;
        }

        lxb_css_parser_clean(parser);
    }

    parser->old_memory = parser->memory;
    parser->old_selectors = parser->selectors;

    parser->selectors = selectors;
    parser->memory = memory;

    parser->tkz->with_comment = false;
    parser->stage = LXB_CSS_PARSER_RUN;

    return LXB_STATUS_OK;
}

lxb_css_stylesheet_t *
lxb_css_stylesheet_process(lxb_css_parser_t *parser,
                           const lxb_char_t *data, size_t length)
{
    lxb_css_stylesheet_t *sst;
    lxb_css_syntax_rule_t *stack;

    lxb_css_parser_buffer_set(parser, data, length);

    stack = lxb_css_syntax_parser_list_rules_push(parser, NULL, NULL,
                                                  &lxb_css_stylesheet_list_rules,
                                                  NULL, true,
                                                  LXB_CSS_SYNTAX_TOKEN_UNDEF);
    if (stack == NULL) {
        return NULL;
    }

    parser->status = lxb_css_syntax_parser_run(parser);
    if (parser->status != LXB_STATUS_OK) {
        return NULL;
    }

    sst = lxb_css_stylesheet_create(parser->memory);
    if (sst == NULL) {
        parser->status = LXB_STATUS_ERROR_MEMORY_ALLOCATION;
        return NULL;
    }

    sst->root = parser->context;
    sst->memory = parser->memory;

    (void) lxb_css_rule_ref_inc(sst->root);

    return sst;
}

void
lxb_css_stylesheet_finish(lxb_css_parser_t *parser)
{
    parser->memory = parser->old_memory;
    parser->selectors = parser->old_selectors;
    parser->stage = LXB_CSS_PARSER_END;
}

lxb_css_stylesheet_t *
lxb_css_stylesheet_parse(lxb_css_parser_t *parser,
                         const lxb_char_t *data, size_t length)
{
    lxb_css_memory_t *memory;
    lxb_css_selectors_t *selectors;
    lxb_css_stylesheet_t *list;

    memory = parser->memory;
    selectors = parser->selectors;

    if (selectors == NULL) {
        selectors = lxb_css_selectors_create();
        parser->status = lxb_css_selectors_init(selectors);

        if (parser->status != LXB_STATUS_OK) {
            (void) lxb_css_selectors_destroy(selectors, true);
            return NULL;
        }
    }
    else {
        lxb_css_selectors_clean(selectors);
    }

    if (memory == NULL) {
        memory = lxb_css_memory_create();
        parser->status = lxb_css_memory_init(memory, 1024);

        if (parser->status != LXB_STATUS_OK) {
            if (selectors != parser->selectors) {
                (void) lxb_css_selectors_destroy(selectors, true);
            }

            (void) lxb_css_memory_destroy(memory, true);
            return NULL;
        }
    }

    parser->status = lxb_css_stylesheet_prepare(parser, memory, selectors);
    if (parser->status != LXB_STATUS_OK) {
        list = NULL;
        goto end;
    }

    list = lxb_css_stylesheet_process(parser, data, length);

    lxb_css_stylesheet_finish(parser);

end:

    if (list == NULL && memory != parser->memory) {
        (void) lxb_css_memory_destroy(memory, true);
    }

    if (selectors != parser->selectors) {
        (void) lxb_css_selectors_destroy(selectors, true);
    }

    return list;
}

static bool
lxb_css_stylesheet_list_rules_state(lxb_css_parser_t *parser,
                                    const lxb_css_syntax_token_t *token, void *ctx)
{
    lxb_css_rule_list_t *list;

    list = lxb_css_rule_list_create(parser->memory);
    if (list == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    lxb_css_parser_current_rule(parser)->context = list;

    return lxb_css_parser_success(parser);
}

static bool
lxb_css_stylesheet_list_rules_next(lxb_css_parser_t *parser,
                                   const lxb_css_syntax_token_t *token, void *ctx)
{
    lxb_css_rule_list_append(ctx, parser->context);

    return lxb_css_parser_success(parser);
}

static lxb_status_t
lxb_css_stylesheet_list_rules_end(lxb_css_parser_t *parser,
                                  const lxb_css_syntax_token_t *token,
                                  void *ctx, bool failed)
{
    if (parser->context != NULL) {
        lxb_css_rule_list_append(ctx, parser->context);
    }

    parser->context = ctx;

    return LXB_STATUS_OK;
}

static bool
lxb_css_stylesheet_at_rule_state(lxb_css_parser_t *parser,
                                 const lxb_css_syntax_token_t *token, void *ctx)
{
    void *value;
    lxb_css_rule_at_t *at_rule;
    const lxb_css_entry_data_t *entry;

    entry = lxb_css_at_rule_by_name(lxb_css_syntax_token_at_keyword(token)->data,
                                    lxb_css_syntax_token_at_keyword(token)->length);
    if (entry == NULL) {
        entry = lxb_css_at_rule_by_id(LXB_CSS_AT_RULE__CUSTOM);
    }

    at_rule = lxb_css_rule_at_create(parser->memory);
    if (at_rule == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    value = entry->create(parser->memory);
    if (value == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    at_rule->type = entry->unique;
    at_rule->u.user = value;

    lxb_css_parser_current_rule(parser)->context = at_rule;

    lxb_css_parser_state_set(parser, entry->state);

    if (entry->unique != LXB_CSS_AT_RULE__CUSTOM) {
        lxb_css_syntax_parser_consume(parser);
        return true;
    }

    return false;
}

static bool
lxb_css_stylesheet_at_rule_block(lxb_css_parser_t *parser,
                                 const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}

static lxb_status_t
lxb_css_stylesheet_at_rule_end(lxb_css_parser_t *parser,
                               const lxb_css_syntax_token_t *token,
                               void *ctx, bool failed)
{
    lxb_css_rule_at_t *at_rule = ctx;
    lxb_css_at_rule__undef_t *undef;
    const lxb_css_syntax_at_rule_offset_t *offset;

    parser->context = ctx;

    if (!failed) {
        return LXB_STATUS_OK;
    }

    parser->status = LXB_STATUS_OK;

    /* Custom rule can't be here. */

    /* Always true. */

    if (at_rule->type != LXB_CSS_AT_RULE__UNDEF) {
        (void) lxb_css_at_rule_destroy(parser->memory, at_rule->u.user,
                                       at_rule->type, true);

        at_rule->u.undef = lxb_css_at_rule__undef_create(parser->memory);
        if (at_rule->u.undef == NULL) {
            return lxb_css_parser_memory_fail_status(parser);
        }
    }

    undef = at_rule->u.undef;
    undef->type = at_rule->type;
    at_rule->type = LXB_CSS_AT_RULE__UNDEF;

    offset = lxb_css_parser_at_rule_offset(parser);

    return lxb_css_at_rule__undef_make(parser, undef, offset);
}

static bool
lxb_css_stylesheet_qualified_rule_state(lxb_css_parser_t *parser,
                                 const lxb_css_syntax_token_t *token, void *ctx)
{
    lxb_css_selectors_clean(parser->selectors);
    lxb_css_parser_current_rule(parser)->context = NULL;

    lxb_css_parser_state_set(parser, lxb_css_selectors_state_complex_list);

    return false;
}

static bool
lxb_css_stylesheet_qualified_rule_block(lxb_css_parser_t *parser,
                                        const lxb_css_syntax_token_t *token,
                                        void *ctx)
{
    lxb_css_syntax_rule_t *stack;
    lxb_css_rule_declaration_list_t *list;

    if (lxb_css_parser_is_failed(parser)) {
        parser->selectors->failed = true;
    }

    list = lxb_css_rule_declaration_list_create(parser->memory);
    if (list == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    lxb_css_parser_current_rule(parser)->context = list;

    if (token->type == LXB_CSS_SYNTAX_TOKEN__END) {
        return lxb_css_parser_success(parser);
    }

    stack = lxb_css_syntax_parser_declarations_push(parser, token,
                                       lxb_css_stylesheet_qualified_rule_back,
                                       &lxb_css_stylesheet_declarations, NULL,
                                       LXB_CSS_SYNTAX_TOKEN_RC_BRACKET);
    if (stack == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    return true;
}

static bool
lxb_css_stylesheet_qualified_rule_back(lxb_css_parser_t *parser,
                                       const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_success(parser);
}

static lxb_status_t
lxb_css_stylesheet_qualified_rule_end(lxb_css_parser_t *parser,
                                      const lxb_css_syntax_token_t *token,
                                      void *ctx, bool failed)
{
    lxb_status_t status;
    lxb_css_rule_style_t *style;
    lxb_css_rule_bad_style_t *bad;
    const lxb_css_syntax_qualified_offset_t *offset;

    if (!failed && !parser->selectors->failed) {
        style = lxb_css_rule_style_create(parser->memory);
        if (style == NULL) {
            return lxb_css_parser_memory_fail_status(parser);
        }

        style->selector = parser->selectors->list;
        style->declarations = ctx;

        parser->context = style;

        return LXB_STATUS_OK;
    }

    bad = lxb_css_rule_bad_style_create(parser->memory);
    if (bad == NULL) {
        return lxb_css_parser_memory_fail_status(parser);
    }

    bad->declarations = ctx;

    offset = lxb_css_parser_qualified_rule_offset(parser);

    status = lxb_css_make_data(parser, &bad->selectors,
                               offset->prelude, offset->prelude_end);
    if (status != LXB_STATUS_OK) {
        return lxb_css_parser_memory_fail_status(parser);
    }

    parser->context = bad;
    parser->status = LXB_STATUS_OK;

    return LXB_STATUS_OK;
}

static bool
lxb_css_stylesheet_declarations_name(lxb_css_parser_t *parser,
                                     const lxb_css_syntax_token_t *token, void *ctx)
{
    void *prop;
    const lxb_css_entry_data_t *entry;
    lxb_css_property__custom_t *custom;
    lxb_css_rule_declaration_t *declar;

    declar = lxb_css_rule_declaration_create(parser->memory);
    if (declar == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    lxb_css_rule_declaration_list_append(parser->rules[-1].context,
                                         &declar->rule);

    entry = lxb_css_property_by_name(lxb_css_syntax_token_ident(token)->data,
                                     lxb_css_syntax_token_ident(token)->length);
    if (entry == NULL) {
        entry = lxb_css_property_by_id(LXB_CSS_PROPERTY__CUSTOM);

        prop = entry->create(parser->memory);
        if (prop == NULL) {
            return lxb_css_parser_memory_fail(parser);
        }

        custom = prop;

        (void) lexbor_str_init(&custom->name, parser->memory->mraw,
                               lxb_css_syntax_token_ident(token)->length);
        if (custom->name.data == NULL) {
            return lxb_css_parser_memory_fail(parser);
        }

        memcpy(custom->name.data, lxb_css_syntax_token_ident(token)->data,
               lxb_css_syntax_token_ident(token)->length);

        custom->name.length = lxb_css_syntax_token_ident(token)->length;
        custom->name.data[custom->name.length] = 0x00;
    }
    else {
        prop = entry->create(parser->memory);
        if (prop == NULL) {
            return lxb_css_parser_memory_fail(parser);
        }
    }

    declar->type = entry->unique;
    declar->u.user = prop;

    lxb_css_parser_state_value_set(parser, entry->state);

    lxb_css_parser_current_rule(parser)->context = declar;
    lxb_css_syntax_parser_consume(parser);

    return lxb_css_parser_success(parser);
}

static bool
lxb_css_stylesheet_declarations_value(lxb_css_parser_t *parser,
                                      const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}

static lxb_status_t
lxb_css_stylesheet_declaration_end(lxb_css_parser_t *parser, void *ctx,
                                   bool important, bool failed)
{
    lxb_css_rule_declaration_t *declar = ctx;

    declar->important = important;

    lxb_css_parser_current_rule(parser)->context = NULL;

    return LXB_STATUS_OK;
}

static lxb_status_t
lxb_css_stylesheet_declarations_end(lxb_css_parser_t *parser,
                                    const lxb_css_syntax_token_t *token,
                                    void *ctx, bool failed)
{
    return LXB_STATUS_OK;
}

static bool
lxb_css_stylesheet_declarations_at_rule_state(lxb_css_parser_t *parser,
                                              const lxb_css_syntax_token_t *token, void *ctx)
{
    lxb_css_rule_at_t *at_rule;
    lxb_css_at_rule__custom_t *custom;
    const lxb_css_entry_data_t *entry;

    entry = lxb_css_at_rule_by_id(LXB_CSS_AT_RULE__CUSTOM);

    at_rule = lxb_css_rule_at_create(parser->memory);
    if (at_rule == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    custom = entry->create(parser->memory);
    if (custom == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    at_rule->type = entry->unique;
    at_rule->u.custom = custom;

    lxb_css_parser_current_rule(parser)->context = at_rule;

    lxb_css_parser_state_set(parser, entry->state);

    return false;
}

static bool
lxb_css_stylesheet_declarations_at_rule_block(lxb_css_parser_t *parser,
                                              const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}

static lxb_status_t
lxb_css_stylesheet_declarations_at_rule_end(lxb_css_parser_t *parser,
                                            const lxb_css_syntax_token_t *token,
                                            void *ctx, bool failed)
{
    lxb_css_rule_at_t *at_rule = ctx;

    lxb_css_rule_declaration_list_append(parser->rules[-2].context,
                                         &at_rule->rule);
    return LXB_STATUS_OK;
}

static bool
lxb_css_stylesheet_declarations_bad(lxb_css_parser_t *parser,
                                    const lxb_css_syntax_token_t *token, void *ctx)
{
    uintptr_t begin, end;
    lxb_status_t status;
    lxb_css_property_type_t type;
    lxb_css_property__undef_t *undef;
    lxb_css_rule_declaration_t *declar = ctx;
    const lxb_css_syntax_declarations_offset_t *offset;

    while (token != NULL && token->type != LXB_CSS_SYNTAX_TOKEN__END) {
        lxb_css_syntax_parser_consume(parser);
        token = lxb_css_syntax_parser_token(parser);
    }

    type = LXB_CSS_PROPERTY__UNDEF;
    
    if (declar == NULL || declar->type != LXB_CSS_PROPERTY__UNDEF) {
        if (declar != NULL) {
            type = declar->type;

            (void) lxb_css_property_destroy(parser->memory, declar->u.user,
                                            declar->type, true);
        }
        else {
            declar = lxb_css_rule_declaration_create(parser->memory);
            if (declar == NULL) {
                return lxb_css_parser_memory_fail(parser);
            }

            lxb_css_rule_declaration_list_append(parser->rules[-1].context,
                                                 &declar->rule);

            lxb_css_parser_current_rule(parser)->context = declar;
        }

        undef = lxb_css_property__undef_create(parser->memory);
        if (undef == NULL) {
            return lxb_css_parser_memory_fail(parser);
        }

        undef->type = type;

        declar->type = LXB_CSS_PROPERTY__UNDEF;
        declar->u.undef = undef;
    }

    undef = declar->u.undef;
    offset = lxb_css_parser_declarations_offset(parser);

    if (type != LXB_CSS_PROPERTY__UNDEF) {
        begin = offset->value_begin;
        end = (offset->before_important) ? offset->before_important
                                         : offset->value_end;
    }
    else {
        begin = offset->name_begin;

        if (offset->before_important) {
            end = offset->before_important;
        }
        else {
            end = (offset->value_end) ? offset->value_end : offset->name_end;
        }
    }

    if (end != 0) {
        status = lxb_css_make_data(parser, &undef->value, begin, end);
        if (status != LXB_STATUS_OK) {
            return lxb_css_parser_memory_fail(parser);
        }
    }

    return lxb_css_parser_success(parser);
}
