/*
 * Copyright (C) 2022 Alexander Borisov
 *
 * Author: Alexander Borisov <borisov@lexbor.com>
 */

#include "lexbor/css/declaration.h"


static bool
lxb_css_declaration_list_name(lxb_css_parser_t *parser,
                              const lxb_css_syntax_token_t *token, void *ctx);
static bool
lxb_css_declaration_list_value(lxb_css_parser_t *parser,
                               const lxb_css_syntax_token_t *token, void *ctx);
static lxb_status_t
lxb_css_declaration_list_end(lxb_css_parser_t *parser, void *ctx,
                             bool important, bool failed);
static lxb_status_t
lxb_css_declarations_list_end(lxb_css_parser_t *parser,
                              const lxb_css_syntax_token_t *token,
                              void *ctx, bool failed);
static bool
lxb_css_declaration_list_at_rule_block(lxb_css_parser_t *parser,
                                       const lxb_css_syntax_token_t *token, void *ctx);
static bool
lxb_css_declaration_list_at_rule_state(lxb_css_parser_t *parser,
                                       const lxb_css_syntax_token_t *token, void *ctx);
static bool
lxb_css_declaration_list_bad(lxb_css_parser_t *parser,
                             const lxb_css_syntax_token_t *token, void *ctx);
static lxb_status_t
lxb_css_declaration_list_at_rule_end(lxb_css_parser_t *parser,
                                     const lxb_css_syntax_token_t *token,
                                     void *ctx, bool failed);


static const lxb_css_syntax_cb_at_rule_t lxb_css_declaration_list_at_cb = {
    .state = lxb_css_declaration_list_at_rule_state,
    .block = lxb_css_declaration_list_at_rule_block,
    .failed = lxb_css_state_failed,
    .end = lxb_css_declaration_list_at_rule_end
};

static const lxb_css_syntax_cb_declarations_t lxb_css_declaration_list_cb = {
    .cb.state = lxb_css_declaration_list_name,
    .cb.block = lxb_css_declaration_list_value,
    .cb.failed = lxb_css_declaration_list_bad,
    .cb.end = lxb_css_declarations_list_end,
    .declaration_end = lxb_css_declaration_list_end,
    .at_rule = &lxb_css_declaration_list_at_cb
};


lxb_status_t
lxb_css_declaration_list_prepare(lxb_css_parser_t *parser,
                                 lxb_css_memory_t *mem)
{
    if (parser->stage != LXB_CSS_PARSER_CLEAN) {
        if (parser->stage == LXB_CSS_PARSER_RUN) {
            return LXB_STATUS_ERROR_WRONG_ARGS;
        }

        lxb_css_parser_clean(parser);
    }

    parser->old_memory = parser->memory;
    parser->memory = mem;

    parser->tkz->with_comment = false;
    parser->stage = LXB_CSS_PARSER_RUN;

    return LXB_STATUS_OK;
}

lxb_css_rule_declaration_list_t *
lxb_css_declaration_list_process(lxb_css_parser_t *parser,
                                 const lxb_char_t *data, size_t length)
{
    lxb_css_syntax_rule_t *stack;
    lxb_css_rule_declaration_list_t *list;

    lxb_css_parser_buffer_set(parser, data, length);

    list = lxb_css_rule_declaration_list_create(parser->memory);
    if (list == NULL) {
        parser->status = LXB_STATUS_ERROR_MEMORY_ALLOCATION;
        return NULL;
    }

    parser->rules->context = list;

    stack = lxb_css_syntax_parser_declarations_push(parser, NULL, NULL,
                                                    &lxb_css_declaration_list_cb,
                                                    NULL, LXB_CSS_SYNTAX_TOKEN_UNDEF);
    if (stack == NULL) {
        goto failed;
    }

    parser->status = lxb_css_syntax_parser_run(parser);
    if (parser->status != LXB_STATUS_OK) {
        goto failed;
    }

    return list;

failed:

    return lxb_css_rule_declaration_list_destroy(list, true);
}

void
lxb_css_declaration_list_finish(lxb_css_parser_t *parser)
{
    parser->memory = parser->old_memory;
    parser->stage = LXB_CSS_PARSER_END;
}

lxb_css_rule_declaration_list_t *
lxb_css_declaration_list_parse(lxb_css_parser_t *parser, lxb_css_memory_t *mem,
                               const lxb_char_t *data, size_t length)
{
    lxb_css_rule_declaration_list_t *list;

    parser->status = lxb_css_declaration_list_prepare(parser, mem);
    if (parser->status != LXB_STATUS_OK) {
        return NULL;
    }

    list = lxb_css_declaration_list_process(parser, data, length);

    lxb_css_declaration_list_finish(parser);

    return list;
}

static bool
lxb_css_declaration_list_name(lxb_css_parser_t *parser,
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
lxb_css_declaration_list_value(lxb_css_parser_t *parser,
                               const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}

static lxb_status_t
lxb_css_declaration_list_end(lxb_css_parser_t *parser, void *ctx,
                             bool important, bool failed)
{
    lxb_css_rule_declaration_t *declar = ctx;

    declar->important = important;

    lxb_css_parser_current_rule(parser)->context = NULL;

    return LXB_STATUS_OK;
}

static lxb_status_t
lxb_css_declarations_list_end(lxb_css_parser_t *parser,
                              const lxb_css_syntax_token_t *token,
                              void *ctx, bool failed)
{
    return LXB_STATUS_OK;
}

static bool
lxb_css_declaration_list_at_rule_state(lxb_css_parser_t *parser,
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
lxb_css_declaration_list_at_rule_block(lxb_css_parser_t *parser,
                                       const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}

static lxb_status_t
lxb_css_declaration_list_at_rule_end(lxb_css_parser_t *parser,
                                     const lxb_css_syntax_token_t *token,
                                     void *ctx, bool failed)
{
    lxb_css_rule_at_t *at_rule = ctx;

    lxb_css_rule_declaration_list_append(parser->rules[-2].context,
                                         &at_rule->rule);
    return LXB_STATUS_OK;
}

static bool
lxb_css_declaration_list_bad(lxb_css_parser_t *parser,
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
