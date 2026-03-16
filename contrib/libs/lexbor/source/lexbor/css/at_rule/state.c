/*
 * Copyright (C) 2021-2022 Alexander Borisov
 *
 * Author: Alexander Borisov <borisov@lexbor.com>
 */

#include "lexbor/css/css.h"
#include "lexbor/css/at_rule.h"
#include "lexbor/css/parser.h"
#include "lexbor/css/rule.h"
#include "lexbor/css/at_rule/state.h"
#include "lexbor/css/at_rule/res.h"


static bool
lxb_css_property_state__custom_block(lxb_css_parser_t *parser,
                                     const lxb_css_syntax_token_t *token,
                                     void *ctx);


bool
lxb_css_at_rule_state__undef(lxb_css_parser_t *parser,
                             const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}

bool
lxb_css_at_rule_state__custom(lxb_css_parser_t *parser,
                              const lxb_css_syntax_token_t *token, void *ctx)
{
    lxb_status_t status;
    lxb_css_rule_at_t *at = ctx;
    lxb_css_at_rule__custom_t *custom = at->u.custom;

    /* Name. */

    (void) lexbor_str_init(&custom->name, parser->memory->mraw,
                           lxb_css_syntax_token_at_keyword(token)->length);
    if (custom->name.data == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    memcpy(custom->name.data, lxb_css_syntax_token_at_keyword(token)->data,
           lxb_css_syntax_token_at_keyword(token)->length);

    custom->name.length = lxb_css_syntax_token_at_keyword(token)->length;
    custom->name.data[custom->name.length] = 0x00;

    /* Prelude. */

    (void) lexbor_str_init(&custom->prelude, parser->memory->mraw, 0);
    if (custom->prelude.data == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    lxb_css_syntax_parser_consume(parser);
    token = lxb_css_syntax_parser_token(parser);

    while (token != NULL && token->type != LXB_CSS_SYNTAX_TOKEN__END) {
        status = lxb_css_syntax_token_serialize_str(token, &custom->prelude,
                                                    parser->memory->mraw);
        if (status != LXB_STATUS_OK) {
            return lxb_css_parser_memory_fail(parser);
        }

        lxb_css_syntax_parser_consume(parser);
        token = lxb_css_syntax_parser_token(parser);
    }

    lxb_css_parser_state_value_set(parser,
                                   lxb_css_property_state__custom_block);

    return lxb_css_parser_success(parser);
}

static bool
lxb_css_property_state__custom_block(lxb_css_parser_t *parser,
                                     const lxb_css_syntax_token_t *token,
                                     void *ctx)
{
    lxb_status_t status;
    lxb_css_rule_at_t *at = ctx;
    lxb_css_at_rule__custom_t *custom = at->u.custom;

    (void) lexbor_str_init(&custom->block, parser->memory->mraw, 0);
    if (custom->block.data == NULL) {
        return lxb_css_parser_memory_fail(parser);
    }

    while (token != NULL && token->type != LXB_CSS_SYNTAX_TOKEN__END) {
        status = lxb_css_syntax_token_serialize_str(token, &custom->block,
                                                    parser->memory->mraw);
        if (status != LXB_STATUS_OK) {
            return lxb_css_parser_memory_fail(parser);
        }

        lxb_css_syntax_parser_consume(parser);
        token = lxb_css_syntax_parser_token(parser);
    }

    return lxb_css_parser_success(parser);
}

bool
lxb_css_at_rule_state_media(lxb_css_parser_t *parser,
                            const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}

bool
lxb_css_at_rule_state_namespace(lxb_css_parser_t *parser,
                                const lxb_css_syntax_token_t *token, void *ctx)
{
    return lxb_css_parser_failed(parser);
}
