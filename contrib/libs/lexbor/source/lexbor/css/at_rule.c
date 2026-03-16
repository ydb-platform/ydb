/*
 * Copyright (C) 2021 Alexander Borisov
 *
 * Author: Alexander Borisov <borisov@lexbor.com>
 */

#include "lexbor/css/at_rule.h"
#include "lexbor/css/css.h"
#include "lexbor/css/parser.h"
#include "lexbor/css/stylesheet.h"
#include "lexbor/css/at_rule/state.h"
#include "lexbor/css/at_rule/res.h"
#include "lexbor/core/serialize.h"


const lxb_css_entry_data_t *
lxb_css_at_rule_by_name(const lxb_char_t *name, size_t length)
{
    const lexbor_shs_entry_t *entry;

    entry = lexbor_shs_entry_get_lower_static(lxb_css_at_rule_shs,
                                              name, length);
    if (entry == NULL) {
        return NULL;
    }

    return entry->value;
}

const lxb_css_entry_data_t *
lxb_css_at_rule_by_id(uintptr_t id)
{
    return &lxb_css_at_rule_data[id];
}

void *
lxb_css_at_rule_destroy(lxb_css_memory_t *memory, void *value,
                        lxb_css_at_rule_type_t type, bool self_destroy)
{
    const lxb_css_entry_data_t *data;

    data = lxb_css_at_rule_by_id(type);
    if (data == NULL) {
        return value;
    }

    return data->destroy(memory, value, self_destroy);
}

lxb_status_t
lxb_css_at_rule_serialize(const void *style, lxb_css_at_rule_type_t type,
                          lexbor_serialize_cb_f cb, void *ctx)
{
    const lxb_css_entry_data_t *data;

    data = lxb_css_at_rule_by_id(type);
    if (data == NULL) {
        return LXB_STATUS_ERROR_UNEXPECTED_DATA;
    }

    return data->serialize(style, cb, ctx);
}

lxb_status_t
lxb_css_at_rule_serialize_str(const void *style, lxb_css_at_rule_type_t type,
                              lexbor_mraw_t *mraw, lexbor_str_t *str)
{
    const lxb_css_entry_data_t *data;

    data = lxb_css_at_rule_by_id(type);
    if (data == NULL) {
        return LXB_STATUS_ERROR_UNEXPECTED_DATA;
    }

    return lxb_css_serialize_str_handler(style, str, mraw, data->serialize);
}

lxb_status_t
lxb_css_at_rule_serialize_name(const void *style, lxb_css_at_rule_type_t type,
                               lexbor_serialize_cb_f cb, void *ctx)
{
    const lxb_css_entry_data_t *data;

    if (type == LXB_CSS_AT_RULE__UNDEF) {
        return lxb_css_at_rule__undef_serialize_name(style, cb, ctx);
    }
    else if (type == LXB_CSS_AT_RULE__CUSTOM) {
        return lxb_css_at_rule__custom_serialize_name(style, cb, ctx);
    }

    data = lxb_css_at_rule_by_id(type);
    if (data == NULL) {
        return LXB_STATUS_ERROR_UNEXPECTED_DATA;
    }

    return cb(data->name, data->length, ctx);
}

lxb_status_t
lxb_css_at_rule_serialize_name_str(const void *style, lxb_css_at_rule_type_t type,
                                   lexbor_mraw_t *mraw, lexbor_str_t *str)
{
    const lxb_css_entry_data_t *data;

    if (type == LXB_CSS_AT_RULE__UNDEF) {
        return lxb_css_serialize_str_handler(style, str, mraw,
                                        lxb_css_at_rule__undef_serialize_name);
    }
    else if (type == LXB_CSS_AT_RULE__CUSTOM) {
        return lxb_css_serialize_str_handler(style, str, mraw,
                                       lxb_css_at_rule__custom_serialize_name);
    }

    data = lxb_css_at_rule_by_id(type);
    if (data == NULL) {
        return LXB_STATUS_ERROR_UNEXPECTED_DATA;
    }

    if (str->data == NULL) {
        lexbor_str_init(str, mraw, data->length);
        if (str->data == NULL) {
            return LXB_STATUS_ERROR_MEMORY_ALLOCATION;
        }
    }

    (void) lexbor_str_append(str, mraw, data->name, data->length);

    return LXB_STATUS_OK;
}

/* _undef. */

void *
lxb_css_at_rule__undef_create(lxb_css_memory_t *memory)
{
    return lexbor_mraw_calloc(memory->mraw, sizeof(lxb_css_at_rule__undef_t));
}

void *
lxb_css_at_rule__undef_destroy(lxb_css_memory_t *memory,
                               void *style, bool self_destroy)
{
    if (style == NULL) {
        return NULL;
    }

    if (self_destroy) {
        return lexbor_mraw_free(memory->mraw, style);
    }

    return style;
}

lxb_status_t
lxb_css_at_rule__undef_make(lxb_css_parser_t *parser,
                            lxb_css_at_rule__undef_t *undef,
                            const lxb_css_syntax_at_rule_offset_t *at_rule)
{
    lxb_status_t status;

    status = lxb_css_make_data(parser, &undef->prelude,
                               at_rule->prelude, at_rule->prelude_end);
    if (status != LXB_STATUS_OK) {
        return status;
    }

    if (at_rule->block != 0) {
        return lxb_css_make_data(parser, &undef->block,
                                 at_rule->block, at_rule->block_end);
    }

    return LXB_STATUS_OK;
}

lxb_status_t
lxb_css_at_rule__undef_serialize(const void *at, lexbor_serialize_cb_f cb,
                                 void *ctx)
{
    lxb_status_t status;
    const lxb_css_entry_data_t *data;
    const lxb_css_at_rule__undef_t *undef = at;

    static const lxb_char_t lb_str[] = "{";
    static const lxb_char_t rb_str[] = "}";
    static const lxb_char_t sm_str[] = ";";

    data = lxb_css_at_rule_by_id(undef->type);
    if (data == NULL) {
        return LXB_STATUS_ERROR_UNEXPECTED_DATA;
    }

    if (undef->prelude.data != NULL) {
        lexbor_serialize_write(cb, undef->prelude.data, undef->prelude.length,
                               ctx, status);
    }

    if (undef->block.data != NULL) {
        lexbor_serialize_write(cb, lb_str, (sizeof(lb_str) - 1), ctx, status);
        lexbor_serialize_write(cb, undef->block.data, undef->block.length,
                               ctx, status);
        lexbor_serialize_write(cb, rb_str, (sizeof(rb_str) - 1), ctx, status);
    }
    else {
        lexbor_serialize_write(cb, sm_str, (sizeof(sm_str) - 1), ctx, status);
    }

    return LXB_STATUS_OK;
}

lxb_status_t
lxb_css_at_rule__undef_serialize_name(const void *at, lexbor_serialize_cb_f cb,
                                      void *ctx)
{
    const lxb_css_entry_data_t *data;
    const lxb_css_at_rule__undef_t *undef = at;

    data = lxb_css_at_rule_by_id(undef->type);
    if (data == NULL) {
        return LXB_STATUS_ERROR_UNEXPECTED_DATA;
    }

    return cb(data->name, data->length, ctx);
}

/* _custom. */

void *
lxb_css_at_rule__custom_create(lxb_css_memory_t *memory)
{
    return lexbor_mraw_calloc(memory->mraw, sizeof(lxb_css_at_rule__custom_t));
}

void *
lxb_css_at_rule__custom_destroy(lxb_css_memory_t *memory,
                                void *style, bool self_destroy)
{
    if (style == NULL) {
        return NULL;
    }

    if (self_destroy) {
        return lexbor_mraw_free(memory->mraw, style);
    }

    return style;
}

lxb_status_t
lxb_css_at_rule__custom_make(lxb_css_parser_t *parser,
                             lxb_css_at_rule__custom_t *custom,
                             const lxb_css_syntax_at_rule_offset_t *at_rule)
{
    lxb_status_t status;

    status = lxb_css_make_data(parser, &custom->name,
                               at_rule->name, at_rule->prelude);
    if (status != LXB_STATUS_OK) {
        return status;
    }

    status = lxb_css_make_data(parser, &custom->prelude,
                               at_rule->prelude, at_rule->prelude_end);
    if (status != LXB_STATUS_OK) {
        return status;
    }

    if (at_rule->block != 0) {
        return lxb_css_make_data(parser, &custom->block,
                                 at_rule->block, at_rule->block_end);
    }

    return LXB_STATUS_OK;
}

lxb_status_t
lxb_css_at_rule__custom_serialize(const void *at, lexbor_serialize_cb_f cb,
                                  void *ctx)
{
    lxb_status_t status;
    const lxb_css_at_rule__custom_t *custom = at;

    static const lxb_char_t lb_str[] = "{";
    static const lxb_char_t rb_str[] = "}";

    if (custom->prelude.data != NULL) {
        lexbor_serialize_write(cb, custom->prelude.data, custom->prelude.length,
                               ctx, status);
    }

    if (custom->block.data != NULL) {
        lexbor_serialize_write(cb, lb_str, (sizeof(lb_str) - 1), ctx, status);
        lexbor_serialize_write(cb, custom->block.data, custom->block.length,
                               ctx, status);
        lexbor_serialize_write(cb, rb_str, (sizeof(rb_str) - 1), ctx, status);
    }

    return LXB_STATUS_OK;
}

lxb_status_t
lxb_css_at_rule__custom_serialize_name(const void *at, lexbor_serialize_cb_f cb,
                                       void *ctx)
{
    const lxb_css_at_rule__custom_t *custom = at;

    return cb(custom->name.data, custom->name.length, ctx);
}

/* Media. */

void *
lxb_css_at_rule_media_create(lxb_css_memory_t *memory)
{
    return lexbor_mraw_calloc(memory->mraw, sizeof(lxb_css_at_rule_media_t));
}

void *
lxb_css_at_rule_media_destroy(lxb_css_memory_t *memory,
                              void *style, bool self_destroy)
{
    return lxb_css_at_rule__undef_destroy(memory, style, self_destroy);
}

lxb_status_t
lxb_css_at_rule_media_serialize(const void *style, lexbor_serialize_cb_f cb,
                                void *ctx)
{
    return LXB_STATUS_OK;
}

/* Namespace. */

void *
lxb_css_at_rule_namespace_create(lxb_css_memory_t *memory)
{
    return lexbor_mraw_calloc(memory->mraw,
                              sizeof(lxb_css_at_rule_namespace_t));
}

void *
lxb_css_at_rule_namespace_destroy(lxb_css_memory_t *memory,
                                  void *style, bool self_destroy)
{
    return lxb_css_at_rule__undef_destroy(memory, style, self_destroy);
}

lxb_status_t
lxb_css_at_rule_namespace_serialize(const void *style, lexbor_serialize_cb_f cb,
                                    void *ctx)
{
    return LXB_STATUS_OK;
}
