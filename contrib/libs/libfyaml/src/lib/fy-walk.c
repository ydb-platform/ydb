/*
 * fy-walk.c - path walker
 *
 * Copyright (c) 2021 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#endif
#include <math.h>
#include <limits.h>

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-doc.h"
#include "fy-walk.h"

#include "fy-utils.h"

#undef DEBUG_EXPR
// #define DEBUG_EXPR

const char *fy_walk_result_type_txt[FWRT_COUNT] = {
	[fwrt_none]	= "none",
	[fwrt_node_ref]	= "node-ref",
	[fwrt_number]	= "number",
	[fwrt_string]	= "string",
	[fwrt_doc]	= "doc",
	[fwrt_refs]	= "refs",
};

void fy_walk_result_dump(struct fy_walk_result *fwr, struct fy_diag *diag, enum fy_error_type errlevel, int level,
		const char *fmt, ...);

void fy_walk_result_vdump(struct fy_walk_result *fwr, struct fy_diag *diag, enum fy_error_type errlevel, int level,
		const char *fmt, va_list ap)
{
	struct fy_walk_result *fwr2;
	char *banner;
	char *texta = NULL;
	const char *text = "";
	size_t len;
	bool save_on_error;
	char buf[30];
	int rc __FY_DEBUG_UNUSED__;

	if (!diag)
		return;

	if (errlevel < diag->cfg.level)
		return;

	save_on_error = diag->on_error;
	diag->on_error = true;

	if (fmt) {
		banner = NULL;
		rc = vasprintf(&banner, fmt, ap);
		assert(rc != -1);
		assert(banner);
		fy_diag_diag(diag, errlevel, "%-*s%s", level*2, "", banner);
		free(banner);
	}
	if (!fwr)
		goto out;

	switch (fwr->type) {
	case fwrt_none:
		text="";
		break;
	case fwrt_node_ref:
		texta = fy_node_get_path(fwr->fyn);
		assert(texta);
		text = texta;
		break;
	case fwrt_number:
		snprintf(buf, sizeof(buf), "%f", fwr->number);
		text = buf;
		break;
	case fwrt_string:
		text = fwr->string;
		break;
	case fwrt_doc:
		texta = fy_emit_document_to_string(fwr->fyd, FYECF_WIDTH_INF | FYECF_INDENT_DEFAULT | FYECF_MODE_FLOW_ONELINE);
		assert(texta);
		text = texta;
		break;
	case fwrt_refs:
		text="";
		break;
	}

	len = strlen(text);

	fy_diag_diag(diag, errlevel, "%-*s%s%s%.*s",
			(level + 1) * 2, "",
			fy_walk_result_type_txt[fwr->type],
			len ? " " : "",
			(int)len, text);

	if (texta)
		free(texta);

	if (fwr->type == fwrt_refs) {
		for (fwr2 = fy_walk_result_list_head(&fwr->refs); fwr2; fwr2 = fy_walk_result_next(&fwr->refs, fwr2))
			fy_walk_result_dump(fwr2, diag, errlevel, level + 1, NULL);
	}
out:
	diag->on_error = save_on_error;
}

void fy_walk_result_dump(struct fy_walk_result *fwr, struct fy_diag *diag, enum fy_error_type errlevel, int level,
		const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_walk_result_vdump(fwr, diag, errlevel, level, fmt, ap);
	va_end(ap);
}

/* NOTE that walk results do not take references and it is invalid to
 * use _any_ call that modifies the document structure
 */
struct fy_walk_result *fy_walk_result_alloc_rl(struct fy_walk_result_list *fwrl)
{
	struct fy_walk_result *fwr = NULL;

	if (fwrl)
		fwr = fy_walk_result_list_pop(fwrl);

	if (!fwr) {
		fwr = malloc(sizeof(*fwr));
		if (!fwr)
			return NULL;
		memset(fwr, 0, sizeof(*fwr));
	}
	fwr->type = fwrt_none;
	return fwr;
}

struct fy_walk_result *fy_walk_result_clone_rl(struct fy_walk_result_list *fwrl, struct fy_walk_result *fwr)
{
	struct fy_walk_result *fwrn = NULL, *fwrn2 = NULL, *fwrn3;

	if (!fwr)
		return NULL;

	fwrn = fy_walk_result_alloc_rl(fwrl);
	if (!fwrn)
		return NULL;

	fwrn->type = fwr->type;
	switch (fwr->type) {
	case fwrt_none:
		break;
	case fwrt_node_ref:
		fwrn->fyn = fwr->fyn;
		break;
	case fwrt_number:
		fwrn->number = fwr->number;
		break;
	case fwrt_string:
		fwrn->string = strdup(fwr->string);
		if (!fwrn->string)
			goto err_out;
		break;
	case fwrt_doc:
		fwrn->fyd = fy_document_clone(fwr->fyd);
		if (!fwrn->fyd)
			goto err_out;
		break;
	case fwrt_refs:

		fy_walk_result_list_init(&fwrn->refs);

		for (fwrn2 = fy_walk_result_list_head(&fwr->refs); fwrn2;
			fwrn2 = fy_walk_result_next(&fwr->refs, fwrn2)) {

			fwrn3 = fy_walk_result_clone_rl(fwrl, fwrn2);
			if (!fwrn3)
				goto err_out;

			fy_walk_result_list_add_tail(&fwrn->refs, fwrn3);
		}
		break;
	}
	return fwrn;
err_out:
	if (fwrn)
		fy_walk_result_free_rl(fwrl, fwrn);
	return NULL;
}

struct fy_walk_result *fy_walk_result_clone(struct fy_walk_result *fwr)
{
	struct fy_walk_result_list *fwrl;

	if (!fwr)
		return NULL;

	fwrl = fy_path_exec_walk_result_rl(fwr->fypx);
	return fy_walk_result_clone_rl(fwrl, fwr);
}

void fy_walk_result_clean_rl(struct fy_walk_result_list *fwrl, struct fy_walk_result *fwr)
{
	struct fy_walk_result *fwrn;

	if (!fwr)
		return;

	switch (fwr->type) {
	case fwrt_none:
		break;
	case fwrt_node_ref:
	case fwrt_number:
		break;
	case fwrt_string:
		if (fwr->string)
			free(fwr->string);
		break;
	case fwrt_doc:
		if (fwr->fyd)
			fy_document_destroy(fwr->fyd);
		break;
	case fwrt_refs:
		while ((fwrn = fy_walk_result_list_pop(&fwr->refs)) != NULL)
			fy_walk_result_free_rl(fwrl, fwrn);
		break;
	}

	fwr->type = fwrt_none;
}

void fy_walk_result_clean(struct fy_walk_result *fwr)
{
	struct fy_walk_result_list *fwrl;

	if (!fwr)
		return;

	fwrl = fy_path_exec_walk_result_rl(fwr->fypx);
	fy_walk_result_clean_rl(fwrl, fwr);
}

void fy_walk_result_free_rl(struct fy_walk_result_list *fwrl, struct fy_walk_result *fwr)
{
	struct fy_path_exec *fypx;

	if (!fwr)
		return;

	fypx = fwr->fypx;

	fy_walk_result_clean_rl(fwrl, fwr);

	if (fwrl)
		fy_walk_result_list_push(fwrl, fwr);
	else
		free(fwr);

	fy_path_exec_unref(fypx);	/* NULL is OK */
}

void fy_walk_result_free(struct fy_walk_result *fwr)
{
	struct fy_walk_result_list *fwrl;

	if (!fwr)
		return;

	fwrl = fy_path_exec_walk_result_rl(fwr->fypx);
	fy_walk_result_free_rl(fwrl, fwr);
}

void fy_walk_result_list_free_rl(struct fy_walk_result_list *fwrl, struct fy_walk_result_list *results)
{
	struct fy_walk_result *fwr;

	while ((fwr = fy_walk_result_list_pop(results)) != NULL)
		fy_walk_result_free_rl(fwrl, fwr);
}

struct fy_walk_result *fy_walk_result_vcreate_rl(struct fy_walk_result_list *fwrl, enum fy_walk_result_type type, va_list ap)
{
	struct fy_walk_result *fwr = NULL;

	if ((unsigned int)type >= FWRT_COUNT)
		goto err_out;

	fwr = fy_walk_result_alloc_rl(fwrl);
	if (!fwr)
		goto err_out;

	fwr->type = type;

	switch (fwr->type) {
	case fwrt_none:
		break;
	case fwrt_node_ref:
		fwr->fyn = va_arg(ap, struct fy_node *);
		break;
	case fwrt_number:
		fwr->number = va_arg(ap, double);
		break;
	case fwrt_string:
		fwr->string = strdup(va_arg(ap, const char *));
		if (!fwr->string)
			goto err_out;
		break;
	case fwrt_doc:
		fwr->fyd = va_arg(ap, struct fy_document *);
		break;
	case fwrt_refs:
		fy_walk_result_list_init(&fwr->refs);
		break;
	}

	return fwr;

err_out:
	fy_walk_result_free_rl(fwrl, fwr);

	return NULL;
}

struct fy_walk_result *fy_walk_result_create_rl(struct fy_walk_result_list *fwrl, enum fy_walk_result_type type, ...)
{
	struct fy_walk_result *fwr;
	va_list ap;

	va_start(ap, type);
	fwr = fy_walk_result_vcreate_rl(fwrl, type, ap);
	va_end(ap);

	return fwr;
}

void fy_walk_result_flatten_internal(struct fy_walk_result *fwr, struct fy_walk_result *fwrf)
{
	struct fy_walk_result *fwr2, *fwr2n;

	if (!fwr || !fwrf || fwr->type != fwrt_refs)
		return;

	for (fwr2 = fy_walk_result_list_head(&fwr->refs); fwr2; fwr2 = fwr2n) {

		fwr2n = fy_walk_result_next(&fwr->refs, fwr2);

		if (fwr2->type != fwrt_refs) {
			fy_walk_result_list_del(&fwr->refs, fwr2);
			fy_walk_result_list_add_tail(&fwrf->refs, fwr2);
			continue;
		}
		fy_walk_result_flatten_internal(fwr2, fwrf);
	}
}

bool fy_walk_result_has_leaves_only(struct fy_walk_result *fwr)
{
	struct fy_walk_result *fwrn;

	if (!fwr || fwr->type != fwrt_refs)
		return false;

	if (fy_walk_result_list_empty(&fwr->refs))
		return false;

	for (fwrn = fy_walk_result_list_head(&fwr->refs); fwrn;
			fwrn = fy_walk_result_next(&fwr->refs, fwrn)) {

		if (fwrn->type == fwrt_refs)
			return false;
	}

	return true;
}

struct fy_walk_result *
fy_walk_result_flatten_rl(struct fy_walk_result_list *fwrl, struct fy_walk_result *fwr)
{
	struct fy_walk_result *fwrf;

	if (!fwr)
		return NULL;

	fwrf = fy_walk_result_create_rl(fwrl, fwrt_refs);
	assert(fwrf);

	fy_walk_result_flatten_internal(fwr, fwrf);
	fy_walk_result_free_rl(fwrl, fwr);
	return fwrf;
}

struct fy_walk_result *
fy_walk_result_flatten(struct fy_walk_result *fwr)
{
	struct fy_walk_result_list *fwrl;

	if (!fwr)
		return NULL;

	fwrl = fy_path_exec_walk_result_rl(fwr->fypx);
	return fy_walk_result_flatten_rl(fwrl, fwr);
}

struct fy_node *
fy_walk_result_node_iterate(struct fy_walk_result *fwr, void **prevp)
{
	struct fy_walk_result *fwrn;

	if (!fwr || !prevp)
		return NULL;

	switch (fwr->type) {
	case fwrt_node_ref:
		if (!*prevp) {
			*prevp = fwr;
			return fwr->fyn;
		}
		*prevp = NULL;
		return NULL;

	case fwrt_refs:
		if (!*prevp)
			fwrn = fy_walk_result_list_head(&fwr->refs);
		else
			fwrn = fy_walk_result_next(&fwr->refs, *prevp);

		/* skip over any non node refs */
		while (fwrn && fwrn->type != fwrt_node_ref)
			fwrn = fy_walk_result_next(&fwr->refs, fwrn);
		*prevp = fwrn;
		return fwrn ? fwrn->fyn : NULL;

	default:
		break;
	}

	return NULL;
}

const char *fy_path_expr_type_txt[FPET_COUNT] = {
	[fpet_none]			= "none",
	/* */
	[fpet_root]			= "root",
	[fpet_this]			= "this",
	[fpet_parent]			= "parent",
	[fpet_every_child]		= "every-child",
	[fpet_every_child_r]		= "every-child-recursive",
	[fpet_filter_collection]	= "filter-collection",
	[fpet_filter_scalar]		= "filter-scalar",
	[fpet_filter_sequence]		= "filter-sequence",
	[fpet_filter_mapping]		= "filter-mapping",
	[fpet_filter_unique]		= "filter-unique",
	[fpet_seq_index]		= "seq-index",
	[fpet_seq_slice]		= "seq-slice",
	[fpet_alias]			= "alias",

	[fpet_map_key]			= "map-key",

	[fpet_multi]			= "multi",
	[fpet_chain]			= "chain",
	[fpet_logical_or]		= "logical-or",
	[fpet_logical_and]		= "logical-and",

	[fpet_eq]			= "equals",
	[fpet_neq]			= "not-equals",
	[fpet_lt]			= "less-than",
	[fpet_gt]			= "greater-than",
	[fpet_lte]			= "less-or-equal-than",
	[fpet_gte]			= "greater-or-equal-than",

	[fpet_scalar]			= "scalar",

	[fpet_plus]			= "plus",
	[fpet_minus]			= "minus",
	[fpet_mult]			= "multiply",
	[fpet_div]			= "divide",

	[fpet_lparen]			= "left-parentheses",
	[fpet_rparen]			= "right-parentheses",
	[fpet_method]			= "method",

	[fpet_scalar_expr]		= "scalar-expression",
	[fpet_path_expr]		= "path-expression",
	[fpet_arg_separator]		= "argument-separator",
};

struct fy_path_expr *fy_path_expr_alloc(void)
{
	struct fy_path_expr *expr = NULL;

	expr = malloc(sizeof(*expr));
	if (!expr)
		return NULL;
	memset(expr, 0, sizeof(*expr));
	fy_path_expr_list_init(&expr->children);

	return expr;
}

void fy_path_expr_free(struct fy_path_expr *expr)
{
	struct fy_path_expr *exprn;

	if (!expr)
		return;

	while ((exprn = fy_path_expr_list_pop(&expr->children)) != NULL)
		fy_path_expr_free(exprn);

	fy_token_unref(expr->fyt);

	free(expr);
}

struct fy_path_expr *fy_path_expr_alloc_recycle(struct fy_path_parser *fypp)
{
	struct fy_path_expr *expr = NULL;

	if (!fypp || fypp->suppress_recycling)
		expr = fy_path_expr_alloc();

	if (!expr) {
		expr = fy_path_expr_list_pop(&fypp->expr_recycle);
		if (expr) {
			memset(expr, 0, sizeof(*expr));
			fy_path_expr_list_init(&expr->children);
		} else
			expr = fy_path_expr_alloc();
	}

	if (!expr)
		return NULL;

	expr->expr_mode = fypp->expr_mode;

	return expr;
}

void fy_path_expr_free_recycle(struct fy_path_parser *fypp, struct fy_path_expr *expr)
{
	struct fy_path_expr *exprn;

	if (!fypp || fypp->suppress_recycling) {
		fy_path_expr_free(expr);
		return;
	}

	while ((exprn = fy_path_expr_list_pop(&expr->children)) != NULL)
		fy_path_expr_free_recycle(fypp, exprn);

	if (expr->fyt) {
		fy_token_unref(expr->fyt);
		expr->fyt = NULL;
	}
	fy_path_expr_list_add_tail(&fypp->expr_recycle, expr);
}

void fy_expr_stack_setup(struct fy_expr_stack *stack)
{
	if (!stack)
		return;

	memset(stack, 0, sizeof(*stack));
	stack->items = stack->items_static;
	stack->alloc = ARRAY_SIZE(stack->items_static);
}

void fy_expr_stack_cleanup(struct fy_expr_stack *stack)
{
	if (!stack)
		return;

	while (stack->top > 0)
		fy_path_expr_free(stack->items[--stack->top]);

	if (stack->items != stack->items_static)
		free(stack->items);
	stack->items = stack->items_static;
	stack->alloc = ARRAY_SIZE(stack->items_static);
}

void fy_expr_stack_dump(struct fy_diag *diag, struct fy_expr_stack *stack)
{
	struct fy_path_expr *expr;
	unsigned int i;

	if (!stack)
		return;

	if (!stack->top)
		return;

	i = stack->top;
	do {
		expr = stack->items[--i];
		fy_path_expr_dump(expr, diag, FYET_NOTICE, 0, NULL);
	} while (i > 0);
}

int fy_expr_stack_size(struct fy_expr_stack *stack)
{
	if (!stack || stack->top >= (unsigned int)INT_MAX)
		return -1;
	return (int)stack->top;
}

int fy_expr_stack_push(struct fy_expr_stack *stack, struct fy_path_expr *expr)
{
	struct fy_path_expr **items_new;
	unsigned int alloc;
	size_t size;

	if (!stack || !expr)
		return -1;

	assert(stack->items);
	assert(stack->alloc > 0);

	assert(expr->fyt);

	/* grow the stack if required */
	if (stack->top >= stack->alloc) {
		alloc = stack->alloc;
		size = alloc * sizeof(*items_new);

		if (stack->items == stack->items_static) {
			items_new = malloc(size * 2);
			if (items_new)
				memcpy(items_new, stack->items_static, size);
		} else
			items_new = realloc(stack->items, size * 2);

		if (!items_new)
			return -1;

		stack->alloc = alloc * 2;
		stack->items = items_new;
	}

	stack->items[stack->top++] = expr;

	return 0;
}

struct fy_path_expr *fy_expr_stack_peek_at(struct fy_expr_stack *stack, unsigned int pos)
{
	if (!stack || stack->top <= pos)
		return NULL;
	return stack->items[stack->top - 1 - pos];
}

struct fy_path_expr *fy_expr_stack_peek(struct fy_expr_stack *stack)
{
	return fy_expr_stack_peek_at(stack, 0);
}

struct fy_path_expr *fy_expr_stack_pop(struct fy_expr_stack *stack)
{
	if (!stack || !stack->top)
		return NULL;

	return stack->items[--stack->top];
}

bool fy_token_type_can_be_path_expr(enum fy_token_type type)
{
	return type == FYTT_NONE ||
	       type == FYTT_PE_LPAREN ||
	       type == FYTT_PE_RPAREN ||
	       type == FYTT_PE_EQEQ ||
	       type == FYTT_PE_NOTEQ ||
	       type == FYTT_PE_GT ||
	       type == FYTT_PE_LT ||
	       type == FYTT_PE_GTE ||
	       type == FYTT_PE_LTE ||
	       type == FYTT_PE_METHOD;
}

bool fy_token_type_can_be_before_negative_number(enum fy_token_type type)
{
	return type == FYTT_NONE ||
	       type == FYTT_PE_LPAREN ||
	       type == FYTT_PE_RPAREN ||
	       type == FYTT_PE_EQEQ ||
	       type == FYTT_PE_NOTEQ ||
	       type == FYTT_PE_GT ||
	       type == FYTT_PE_LT ||
	       type == FYTT_PE_GTE ||
	       type == FYTT_PE_LTE ||
	       type == FYTT_SE_PLUS ||
	       type == FYTT_SE_MINUS ||
	       type == FYTT_SE_MULT ||
	       type == FYTT_SE_DIV ||

	       type == FYTT_SE_METHOD;
}

const char *fy_expr_mode_txt[FYEM_COUNT] = {
	[fyem_none]	= "none",
	[fyem_path]	= "path",
	[fyem_scalar]	= "scalar",
};

static struct fy_diag *fy_path_parser_reader_get_diag(struct fy_reader *fyr)
{
	struct fy_path_parser *fypp = fy_container_of(fyr, struct fy_path_parser, reader);
	return fypp->cfg.diag;
}

static const struct fy_reader_ops fy_path_parser_reader_ops = {
	.get_diag = fy_path_parser_reader_get_diag,
};

void fy_path_parser_setup(struct fy_path_parser *fypp, const struct fy_path_parse_cfg *pcfg)
{
	if (!fypp)
		return;

	memset(fypp, 0, sizeof(*fypp));
	if (pcfg)
		fypp->cfg = *pcfg;
	fy_reader_setup(&fypp->reader, &fy_path_parser_reader_ops);
	fy_token_list_init(&fypp->queued_tokens);
	fypp->last_queued_token_type = FYTT_NONE;

	fy_expr_stack_setup(&fypp->operators);
	fy_expr_stack_setup(&fypp->operands);

	fy_path_expr_list_init(&fypp->expr_recycle);
	fypp->suppress_recycling = (fypp->cfg.flags & FYPPCF_DISABLE_RECYCLING) || getenv("FY_VALGRIND");

	fypp->expr_mode = fyem_path;
	fypp->paren_nest_level = 0;
}

void fy_path_parser_cleanup(struct fy_path_parser *fypp)
{
	struct fy_path_expr *expr;

	if (!fypp)
		return;

	fy_expr_stack_cleanup(&fypp->operands);
	fy_expr_stack_cleanup(&fypp->operators);

	fy_reader_cleanup(&fypp->reader);
	fy_token_list_unref_all(&fypp->queued_tokens);

	while ((expr = fy_path_expr_list_pop(&fypp->expr_recycle)) != NULL)
		fy_path_expr_free(expr);

	fypp->last_queued_token_type = FYTT_NONE;
	fypp->stream_start_produced = false;
	fypp->stream_end_produced = false;
	fypp->stream_error = false;
	fypp->token_activity_counter = 0;
	fypp->paren_nest_level = 0;
}

int fy_path_parser_open(struct fy_path_parser *fypp,
			struct fy_input *fyi, const struct fy_reader_input_cfg *icfg)
{
	int ret;
	if (!fypp)
		return -1;

	ret = fy_reader_input_open(&fypp->reader, fyi, icfg);
	if (ret)
		return ret;
	/* take a reference to the input */
	fypp->fyi = fy_input_ref(fyi);
	return 0;
}

void fy_path_parser_close(struct fy_path_parser *fypp)
{
	if (!fypp)
		return;

	fy_input_unref(fypp->fyi);

	fy_reader_input_done(&fypp->reader);
}

struct fy_token *fy_path_token_vqueue(struct fy_path_parser *fypp, enum fy_token_type type, va_list ap)
{
	struct fy_token *fyt;

	fyt = fy_token_list_vqueue(&fypp->queued_tokens, type, ap);
	if (fyt) {
		fypp->token_activity_counter++;
		fypp->last_queued_token_type = type;
	}
	return fyt;
}

struct fy_token *fy_path_token_queue(struct fy_path_parser *fypp, enum fy_token_type type, ...)
{
	va_list ap;
	struct fy_token *fyt;

	va_start(ap, type);
	fyt = fy_path_token_vqueue(fypp, type, ap);
	va_end(ap);

	return fyt;
}

int fy_path_fetch_seq_index_or_slice(struct fy_path_parser *fypp, int c)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	bool neg;
	int i, j, val, nval, digits, indices[2];

	fyr = &fypp->reader;

	/* verify that the called context is correct */
	assert(fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, 1))));

	i = 0;
	indices[0] = indices[1] = -1;

	j = 0;
	while (j < 2) {

		neg = false;
		if (c == '-') {
			neg = true;
			i++;
		}

		digits = 0;
		val = 0;
		while (fy_is_num((c = fy_reader_peek_at(fyr, i)))) {
			nval = (val * 10) | (c - '0');
			FYR_PARSE_ERROR_CHECK(fyr, 0, i, FYEM_SCAN,
					nval >= val && nval >= 0, err_out,
					"illegal sequence index (overflow)");
			val = nval;
			i++;
			digits++;
		}
		FYR_PARSE_ERROR_CHECK(fyr, 0, i, FYEM_SCAN,
				(val == 0 && digits == 1) || (val > 0), err_out,
				"bad number");
		if (neg)
			val = -val;

		indices[j] = val;

		/* continue only on slice : */
		if (c == ':') {
			c = fy_reader_peek_at(fyr, i + 1);
			if (fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, i + 2)))) {
				i++;
				j++;
				continue;
			}
		}

		break;
	}

	if (j >= 1)
		fyt = fy_path_token_queue(fypp, FYTT_PE_SEQ_SLICE, fy_reader_fill_atom_a(fyr, i), indices[0], indices[1]);
	else
		fyt = fy_path_token_queue(fypp, FYTT_PE_SEQ_INDEX, fy_reader_fill_atom_a(fyr, i), indices[0]);

	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_plain_or_method(struct fy_path_parser *fypp, int c,
				  enum fy_token_type fytt_plain,
				  enum fy_token_type fytt_method)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	struct fy_atom *handlep;
	int i;
	enum fy_token_type type;

	fyr = &fypp->reader;

	assert(fy_is_first_alpha(c));

	type = fytt_plain;

	i = 1;
	while (fy_is_alnum(fy_reader_peek_at(fyr, i)))
		i++;

	if (fy_reader_peek_at(fyr, i) == '(')
		type = fytt_method;

	handlep = fy_reader_fill_atom_a(fyr, i);
	if (type == FYTT_SCALAR) {
		fyt = fy_path_token_queue(fypp, FYTT_SCALAR, handlep, FYSS_PLAIN, NULL);
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");
	} else {
		fyt = fy_path_token_queue(fypp, type, handlep, NULL);
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");
	}

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_dot_method(struct fy_path_parser *fypp, int c, enum fy_token_type fytt)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	struct fy_atom *handlep;
	int i;

	fyr = &fypp->reader;

	assert(c == '.');
	fy_reader_advance(fyr, c);
	c = fy_reader_peek(fyr);
	assert(fy_is_first_alpha(c));

	/* verify that the called context is correct */
	i = 1;
	while (fy_is_alnum(fy_reader_peek_at(fyr, i)))
		i++;

	handlep = fy_reader_fill_atom_a(fyr, i);

	fyt = fy_path_token_queue(fypp, fytt, handlep, NULL);
	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_flow_document(struct fy_path_parser *fypp, int c, enum fy_token_type fytt)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	struct fy_document *fyd;
	struct fy_atom handle;
	struct fy_parser fyp_data, *fyp = &fyp_data;
	struct fy_parse_cfg cfg_data, *cfg = NULL;
	int rc;

	fyr = &fypp->reader;

	/* verify that the called context is correct */
	assert(fy_is_path_flow_key_start(c));

	fy_reader_fill_atom_start(fyr, &handle);

	cfg = &cfg_data;
	memset(cfg, 0, sizeof(*cfg));
	cfg->flags = FYPCF_DEFAULT_PARSE;
	cfg->diag = fypp->cfg.diag;

	rc = fy_parse_setup(fyp, cfg);
	fyr_error_check(fyr, !rc, err_out, "fy_parse_setup() failed\n");

	/* associate with reader and set flow mode */
	fy_parser_set_reader(fyp, fyr);
	fy_parser_set_flow_only_mode(fyp, true);

	fyd = fy_parse_load_document(fyp);

	/* cleanup the parser no matter what */
	fy_parse_cleanup(fyp);

	fyr_error_check(fyr, fyd, err_out, "fy_parse_load_document() failed\n");

	fy_reader_fill_atom_end(fyr, &handle);

	/* document is NULL, is a simple key */
	fyt = fy_path_token_queue(fypp, fytt, &handle, fyd);
	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_flow_map_key(struct fy_path_parser *fypp, int c)
{
	return fy_path_fetch_flow_document(fypp, c, FYTT_PE_MAP_KEY);
}

int fy_path_fetch_flow_scalar(struct fy_path_parser *fypp, int c)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	struct fy_atom handle;
	bool is_single;
	int rc = -1;

	fyr = &fypp->reader;

	/* verify that the called context is correct */
	assert(fy_is_path_flow_scalar_start(c));

	is_single = c == '\'';

	rc = fy_reader_fetch_flow_scalar_handle(fyr, c, 0, &handle, false);
	if (rc)
		goto err_out_rc;

	/* document is NULL, is a simple key */
	fyt = fy_path_token_queue(fypp, FYTT_SCALAR, &handle, is_single ? FYSS_SINGLE_QUOTED : FYSS_DOUBLE_QUOTED);
	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	rc = -1;
err_out_rc:
	fypp->stream_error = true;
	return rc;
}

int fy_path_fetch_number(struct fy_path_parser *fypp, int c)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	int i, digits;

	fyr = &fypp->reader;

	/* verify that the called context is correct */
	assert(fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, 1))));

	i = 0;
	if (c == '-')
		i++;

	digits = 0;
	while (fy_is_num((c = fy_reader_peek_at(fyr, i)))) {
		i++;
		digits++;
	}
	FYR_PARSE_ERROR_CHECK(fyr, 0, i, FYEM_SCAN,
			digits > 0, err_out,
			"bad number");

	fyt = fy_path_token_queue(fypp, FYTT_SCALAR, fy_reader_fill_atom_a(fyr, i), FYSS_PLAIN);
	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_tokens(struct fy_path_parser *fypp)
{
	enum fy_token_type type;
	struct fy_token *fyt;
	struct fy_reader *fyr;
	int c, cn, rc, simple_token_count;

	fyr = &fypp->reader;
	if (!fypp->stream_start_produced) {

		fyt = fy_path_token_queue(fypp, FYTT_STREAM_START, fy_reader_fill_atom_a(fyr, 0));
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

		fypp->stream_start_produced = true;
		return 0;
	}

	/* XXX scan to next token? */

	c = fy_reader_peek(fyr);

	if (fy_is_z(c)) {

		if (c >= 0)
			fy_reader_advance(fyr, c);

		/* produce stream end continuously */
		fyt = fy_path_token_queue(fypp, FYTT_STREAM_END, fy_reader_fill_atom_a(fyr, 0));
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

		return 0;
	}

	fyt = NULL;
	type = FYTT_NONE;
	simple_token_count = 0;

	/* first do the common tokens */
	switch (c) {
	case ',':
		type = FYTT_PE_COMMA;
		simple_token_count = 1;
		break;

	case '|':
		if (fy_reader_peek_at(fyr, 1) == '|') {
			type = FYTT_PE_BARBAR;
			simple_token_count = 2;
			break;
		}
		break;

	case '&':
		if (fy_reader_peek_at(fyr, 1) == '&') {
			type = FYTT_PE_AMPAMP;
			simple_token_count = 2;
			break;
		}
		break;

	case '(':
		type = FYTT_PE_LPAREN;
		simple_token_count = 1;
		break;

	case ')':
		type = FYTT_PE_RPAREN;
		simple_token_count = 1;
		break;

	case '=':
		cn = fy_reader_peek_at(fyr, 1);
		if (cn == '=') {
			type = FYTT_PE_EQEQ;
			simple_token_count = 2;
			break;
		}
		break;

	case '>':
		cn = fy_reader_peek_at(fyr, 1);
		if (cn == '=') {
			type = FYTT_PE_GTE;
			simple_token_count = 2;
			break;
		}
		type = FYTT_PE_GT;
		simple_token_count = 1;
		break;

	case '<':
		cn = fy_reader_peek_at(fyr, 1);
		if (cn == '=') {
			type = FYTT_PE_LTE;
			simple_token_count = 2;
			break;
		}
		type = FYTT_PE_LT;
		simple_token_count = 1;
		break;

	case '!':
		cn = fy_reader_peek_at(fyr, 1);
		if (cn == '=') {
			type = FYTT_PE_NOTEQ;
			simple_token_count = 2;
			break;
		}
		/* may still be something else */
		break;

	default:
		break;
	}

	if (type != FYTT_NONE)
		goto do_token;

again:

	switch (fypp->expr_mode) {
	case fyem_none:
		assert(0);	/* should never happen */
		break;

	case fyem_path:

		switch (c) {
		case '/':
			type = FYTT_PE_SLASH;
			simple_token_count = 1;
			break;

		case '^':
			type = FYTT_PE_ROOT;
			simple_token_count = 1;
			break;

		case ':':
			type = FYTT_PE_SIBLING;
			simple_token_count = 1;
			break;

		case '$':
			type = FYTT_PE_SCALAR_FILTER;
			simple_token_count = 1;
			break;

		case '%':
			type = FYTT_PE_COLLECTION_FILTER;
			simple_token_count = 1;
			break;

		case '[':
			if (fy_reader_peek_at(fyr, 1) == ']') {
				type = FYTT_PE_SEQ_FILTER;
				simple_token_count = 2;
			}
			break;

		case '{':
			if (fy_reader_peek_at(fyr, 1) == '}') {
				type = FYTT_PE_MAP_FILTER;
				simple_token_count = 2;
			}
			break;


		case '.':
			cn = fy_reader_peek_at(fyr, 1);
			if (cn == '.') {
				type = FYTT_PE_PARENT;
				simple_token_count = 2;
			} else if (!fy_is_first_alpha(cn)) {
				type = FYTT_PE_THIS;
				simple_token_count = 1;
			}
			break;

		case '*':
			if (fy_reader_peek_at(fyr, 1) == '*') {
				type = FYTT_PE_EVERY_CHILD_R;
				simple_token_count = 2;
			} else if (!fy_is_first_alpha(fy_reader_peek_at(fyr, 1))) {
				type = FYTT_PE_EVERY_CHILD;
				simple_token_count = 1;
			} else {
				type = FYTT_PE_ALIAS;
				simple_token_count = 2;
				while (fy_is_alnum(fy_reader_peek_at(fyr, simple_token_count)))
					simple_token_count++;
			}
			break;

		case '!':
			cn = fy_reader_peek_at(fyr, 1);
			if (cn == '=') {
				type = FYTT_PE_NOTEQ;
				simple_token_count = 2;
				break;
			}
			type = FYTT_PE_UNIQUE_FILTER;
			simple_token_count = 1;
			break;

		default:
			break;
		}
		break;

	case fyem_scalar:


		/* it is possible for the expression to be a path
		 * we only detect a few cases (doing all too complex)
		 * (/ , (./ , (*
		 */
		if (fy_token_type_can_be_path_expr(fypp->last_queued_token_type)) {
			cn = fy_reader_peek_at(fyr, 1);
			if (c == '/' ||
			    (c == '.' && (cn == '/' || cn == ')' || cn == '>' || cn == '<' || cn == '!' || cn == '='))) {
				fypp->expr_mode = fyem_path;
#ifdef DEBUG_EXPR
				fyr_notice(fyr, "switching to path expr\n");
#endif
				goto again;
			}
		}

		switch (c) {
		case '+':
			type = FYTT_SE_PLUS;
			simple_token_count = 1;
			break;

		case '-':
			cn = fy_reader_peek_at(fyr, 1);
			if (fy_is_num(cn) &&
			    fy_token_type_can_be_before_negative_number(fypp->last_queued_token_type))
				break;

			type = FYTT_SE_MINUS;
			simple_token_count = 1;
			break;

		case '*':
			type = FYTT_SE_MULT;
			simple_token_count = 1;
			break;

		case '/':
			type = FYTT_SE_DIV;
			simple_token_count = 1;
			break;

		default:
			break;
		}
		break;
	}

do_token:
	/* simple tokens */
	if (simple_token_count > 0) {
		fyt = fy_path_token_queue(fypp, type, fy_reader_fill_atom_a(fyr, simple_token_count));
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

		return 0;
	}

	switch (fypp->expr_mode) {
	case fyem_none:
		assert(0);	/* should never happen */
		break;

	case fyem_path:
		if (fy_is_first_alpha(c))
			return fy_path_fetch_plain_or_method(fypp, c, FYTT_PE_MAP_KEY, FYTT_PE_METHOD);

		if (fy_is_path_flow_key_start(c))
			return fy_path_fetch_flow_map_key(fypp, c);

		if (fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, 1))))
			return fy_path_fetch_seq_index_or_slice(fypp, c);

		if (c == '.' && fy_is_first_alpha(fy_reader_peek_at(fyr, 1)))
			return fy_path_fetch_dot_method(fypp, c, FYTT_PE_METHOD);

		break;

	case fyem_scalar:

		if (fy_is_first_alpha(c))
			return fy_path_fetch_plain_or_method(fypp, c, FYTT_SCALAR, FYTT_SE_METHOD);

		if (fy_is_path_flow_scalar_start(c))
			return fy_path_fetch_flow_scalar(fypp, c);

		if (fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, 1))))
			return fy_path_fetch_number(fypp, c);

#if 0
		if (c == '.' && fy_is_first_alpha(fy_reader_peek_at(fyr, 1)))
			return fy_path_fetch_dot_method(fypp, c, FYTT_SE_METHOD);
#endif

		break;
	}

	FYR_PARSE_ERROR(fyr, 0, 1, FYEM_SCAN, "bad path expression starts here c=%d", c);

err_out:
	fypp->stream_error = true;
	rc = -1;
	return rc;
}

struct fy_token *fy_path_scan_peek(struct fy_path_parser *fypp, struct fy_token *fyt_prev)
{
	struct fy_token *fyt;
	struct fy_reader *fyr;
	int rc, last_token_activity_counter;

	fyr = &fypp->reader;

	/* nothing if stream end produced (and no stream end token in queue) */
	if (!fyt_prev && fypp->stream_end_produced && fy_token_list_empty(&fypp->queued_tokens)) {

		fyt = fy_token_list_head(&fypp->queued_tokens);
		if (fyt && fyt->type == FYTT_STREAM_END)
			return fyt;

		return NULL;
	}

	for (;;) {
		if (!fyt_prev)
			fyt = fy_token_list_head(&fypp->queued_tokens);
		else
			fyt = fy_token_next(&fypp->queued_tokens, fyt_prev);
		if (fyt)
			break;

		/* on stream error we're done */
		if (fypp->stream_error)
			return NULL;

		/* keep track of token activity, if it didn't change
		* after the fetch tokens call, the state machine is stuck
		*/
		last_token_activity_counter = fypp->token_activity_counter;

		/* fetch more then */
		rc = fy_path_fetch_tokens(fypp);
		if (rc) {
			fy_error(fypp->cfg.diag, "fy_path_fetch_tokens() failed\n");
			goto err_out;
		}
		if (last_token_activity_counter == fypp->token_activity_counter) {
			fy_error(fypp->cfg.diag, "out of tokens and failed to produce anymore");
			goto err_out;
		}
	}

	switch (fyt->type) {
	case FYTT_STREAM_START:
		fypp->stream_start_produced = true;
		break;
	case FYTT_STREAM_END:
		fypp->stream_end_produced = true;

		rc = fy_reader_input_done(fyr);
		if (rc) {
			fy_error(fypp->cfg.diag, "fy_parse_input_done() failed");
			goto err_out;
		}
		break;
	default:
		break;
	}

	return fyt;

err_out:
	fypp->stream_error = true;
	return NULL;
}

struct fy_token *fy_path_scan_remove(struct fy_path_parser *fypp, struct fy_token *fyt)
{
	if (!fypp || !fyt)
		return NULL;

	fy_token_list_del(&fypp->queued_tokens, fyt);

	return fyt;
}

struct fy_token *fy_path_scan_remove_peek(struct fy_path_parser *fypp, struct fy_token *fyt)
{
	fy_token_unref(fy_path_scan_remove(fypp, fyt));

	return fy_path_scan_peek(fypp, NULL);
}

struct fy_token *fy_path_scan(struct fy_path_parser *fypp)
{
	return fy_path_scan_remove(fypp, fy_path_scan_peek(fypp, NULL));
}

void fy_path_expr_dump(struct fy_path_expr *expr, struct fy_diag *diag, enum fy_error_type errlevel, int level, const char *banner)
{
	struct fy_path_expr *expr2;
	const char *style = "";
	const char *text;
	size_t len;
	bool save_on_error;

	if (errlevel < diag->cfg.level)
		return;

	save_on_error = diag->on_error;
	diag->on_error = true;

	if (banner)
		fy_diag_diag(diag, errlevel, "%-*s%s", level*2, "", banner);

	text = fy_token_get_text(expr->fyt, &len);

	style = "";
	if (expr->type == fpet_scalar) {
		switch (fy_scalar_token_get_style(expr->fyt)) {
		case FYSS_SINGLE_QUOTED:
			style = "'";
			break;
		case FYSS_DOUBLE_QUOTED:
			style = "\"";
			break;
		default:
			style = "";
			break;
		}
	}

	fy_diag_diag(diag, errlevel, "> %-*s%s:%s %s%.*s%s",
			level*2, "",
			fy_path_expr_type_txt[expr->type],
			fy_expr_mode_txt[expr->expr_mode],
			style, (int)len, text, style);

	for (expr2 = fy_path_expr_list_head(&expr->children); expr2; expr2 = fy_path_expr_next(&expr->children, expr2))
		fy_path_expr_dump(expr2, diag, errlevel, level + 1, NULL);

	diag->on_error = save_on_error;
}

static struct fy_node *
fy_path_expr_to_node_internal(struct fy_document *fyd, struct fy_path_expr *expr)
{
	struct fy_path_expr *expr2;
	const char *style = "";
	const char *text;
	size_t len;
	struct fy_node *fyn = NULL, *fyn2, *fyn_seq = NULL;
	int rc;

	text = fy_token_get_text(expr->fyt, &len);

	/* by default use double quoted style */
	style = "\"";
	switch (expr->type) {
	case fpet_scalar:
		switch (fy_scalar_token_get_style(expr->fyt)) {
		case FYSS_SINGLE_QUOTED:
			style = "'";
			break;
		case FYSS_DOUBLE_QUOTED:
			style = "\"";
			break;
		default:
			style = "";
			break;
		}
		break;

	case fpet_map_key:
		/* no styles for complex map keys */
		if (expr->fyt->map_key.fyd)
			style = "";
		break;

	default:
		break;
	}

	/* list is empty this is a terminal */
	if (fy_path_expr_list_empty(&expr->children) &&
	    expr->type != fpet_method) {

		fyn = fy_node_buildf(fyd, "%s: %s%.*s%s",
			fy_path_expr_type_txt[expr->type],
			style, (int)len, text, style);
		if (!fyn)
			return NULL;

		return fyn;
	}

	fyn = fy_node_create_mapping(fyd);
	if (!fyn)
		goto err_out;

	fyn_seq = fy_node_create_sequence(fyd);
	if (!fyn_seq)
		goto err_out;

	for (expr2 = fy_path_expr_list_head(&expr->children); expr2; expr2 = fy_path_expr_next(&expr->children, expr2)) {
		fyn2 = fy_path_expr_to_node_internal(fyd, expr2);
		if (!fyn2)
			goto err_out;
		rc = fy_node_sequence_append(fyn_seq, fyn2);
		if (rc)
			goto err_out;
	}

	if (expr->type != fpet_method) {
		rc = fy_node_mapping_append(fyn,
				fy_node_create_scalar(fyd, fy_path_expr_type_txt[expr->type], FY_NT),
				fyn_seq);
	} else {
		rc = fy_node_mapping_append(fyn,
				fy_node_create_scalarf(fyd, "%s()", expr->fym->name),
				fyn_seq);
	}
	if (rc)
		goto err_out;

	return fyn;

err_out:
	fy_node_free(fyn_seq);
	fy_node_free(fyn);
	return NULL;
}

struct fy_document *fy_path_expr_to_document(struct fy_path_expr *expr)
{
	struct fy_document *fyd = NULL;

	if (!expr)
		return NULL;

	fyd = fy_document_create(NULL);
	if (!fyd)
		return NULL;

	fyd->root = fy_path_expr_to_node_internal(fyd, expr);
	if (!fyd->root)
		goto err_out;

	return fyd;

err_out:
	fy_document_destroy(fyd);
	return NULL;
}

enum fy_path_expr_type fy_map_token_to_path_expr_type(enum fy_token_type type, enum fy_expr_mode mode)
{
	switch (type) {
	case FYTT_PE_ROOT:
		return fpet_root;
	case FYTT_PE_THIS:
		return fpet_this;
	case FYTT_PE_PARENT:
	case FYTT_PE_SIBLING:	/* sibling maps to a chain of fpet_parent */
		return fpet_parent;
	case FYTT_PE_MAP_KEY:
		return fpet_map_key;
	case FYTT_PE_SEQ_INDEX:
		return fpet_seq_index;
	case FYTT_PE_SEQ_SLICE:
		return fpet_seq_slice;
	case FYTT_PE_EVERY_CHILD:
		return fpet_every_child;
	case FYTT_PE_EVERY_CHILD_R:
		return fpet_every_child_r;
	case FYTT_PE_ALIAS:
		return fpet_alias;
	case FYTT_PE_SCALAR_FILTER:
		return fpet_filter_scalar;
	case FYTT_PE_COLLECTION_FILTER:
		return fpet_filter_collection;
	case FYTT_PE_SEQ_FILTER:
		return fpet_filter_sequence;
	case FYTT_PE_MAP_FILTER:
		return fpet_filter_mapping;
	case FYTT_PE_UNIQUE_FILTER:
		return fpet_filter_unique;
	case FYTT_PE_COMMA:
		return mode == fyem_path ? fpet_multi : fpet_arg_separator;
	case FYTT_PE_SLASH:
		return fpet_chain;
	case FYTT_PE_BARBAR:
		return fpet_logical_or;
	case FYTT_PE_AMPAMP:
		return fpet_logical_and;

	case FYTT_PE_EQEQ:
		return fpet_eq;
	case FYTT_PE_NOTEQ:
		return fpet_neq;
	case FYTT_PE_LT:
		return fpet_lt;
	case FYTT_PE_GT:
		return fpet_gt;
	case FYTT_PE_LTE:
		return fpet_lte;
	case FYTT_PE_GTE:
		return fpet_gte;

	case FYTT_SCALAR:
		return fpet_scalar;

	case FYTT_SE_PLUS:
		return fpet_plus;
	case FYTT_SE_MINUS:
		return fpet_minus;
	case FYTT_SE_MULT:
		return fpet_mult;
	case FYTT_SE_DIV:
		return fpet_div;

	case FYTT_PE_LPAREN:
		return fpet_lparen;
	case FYTT_PE_RPAREN:
		return fpet_rparen;

	case FYTT_SE_METHOD:
	case FYTT_PE_METHOD:
		return fpet_method;

	default:
		/* note parentheses do not have an expression */
		assert(0);
		break;
	}
	return fpet_none;
}

bool fy_token_type_is_operand(enum fy_token_type type)
{
	return type == FYTT_PE_ROOT ||
	       type == FYTT_PE_THIS ||
	       type == FYTT_PE_PARENT ||
	       type == FYTT_PE_MAP_KEY ||
	       type == FYTT_PE_SEQ_INDEX ||
	       type == FYTT_PE_SEQ_SLICE ||
	       type == FYTT_PE_EVERY_CHILD ||
	       type == FYTT_PE_EVERY_CHILD_R ||
	       type == FYTT_PE_ALIAS ||

	       type == FYTT_SCALAR;
}

bool fy_token_type_is_operator(enum fy_token_type type)
{
	return type == FYTT_PE_SLASH ||
	       type == FYTT_PE_SCALAR_FILTER ||
	       type == FYTT_PE_COLLECTION_FILTER ||
	       type == FYTT_PE_SEQ_FILTER ||
	       type == FYTT_PE_MAP_FILTER ||
	       type == FYTT_PE_UNIQUE_FILTER ||
	       type == FYTT_PE_SIBLING ||
	       type == FYTT_PE_COMMA ||
	       type == FYTT_PE_BARBAR ||
	       type == FYTT_PE_AMPAMP ||
	       type == FYTT_PE_LPAREN ||
	       type == FYTT_PE_RPAREN ||

	       type == FYTT_PE_EQEQ ||
	       type == FYTT_PE_NOTEQ ||
	       type == FYTT_PE_LT ||
	       type == FYTT_PE_GT ||
	       type == FYTT_PE_LTE ||
	       type == FYTT_PE_GTE ||

	       type == FYTT_SE_PLUS ||
	       type == FYTT_SE_MINUS ||
	       type == FYTT_SE_MULT ||
	       type == FYTT_SE_DIV;
}

bool fy_token_type_is_operand_or_operator(enum fy_token_type type)
{
	return fy_token_type_is_operand(type) ||
	       fy_token_type_is_operator(type);
}

int fy_path_expr_type_prec(enum fy_path_expr_type type)
{
	switch (type) {
	default:
		return -1;	/* terminals */
	case fpet_filter_collection:
	case fpet_filter_scalar:
	case fpet_filter_sequence:
	case fpet_filter_mapping:
	case fpet_filter_unique:
		return 5;
	case fpet_logical_or:
	case fpet_logical_and:
		return 4;
	case fpet_multi:
		return 11;
	case fpet_eq:
	case fpet_neq:
	case fpet_lt:
	case fpet_gt:
	case fpet_lte:
	case fpet_gte:
		return 7;
	case fpet_mult:
	case fpet_div:
		return 9;
	case fpet_plus:
	case fpet_minus:
		return 8;
	case fpet_chain:
		return 10;
	case fpet_lparen:
	case fpet_rparen:
	case fpet_method:
		return 1000;
	case fpet_arg_separator:
		return 1;	/* lowest */
	}
	return -1;
}

static inline FY_UNUSED void
dump_operand_stack(struct fy_path_parser *fypp)
{
	return fy_expr_stack_dump(fypp->cfg.diag, &fypp->operands);
}

static inline int
push_operand(struct fy_path_parser *fypp, struct fy_path_expr *expr)
{
	return fy_expr_stack_push(&fypp->operands, expr);
}

static inline FY_UNUSED struct fy_path_expr *
peek_operand_at(struct fy_path_parser *fypp, unsigned int pos)
{
	return fy_expr_stack_peek_at(&fypp->operands, pos);
}

static inline FY_UNUSED struct fy_path_expr *
peek_operand(struct fy_path_parser *fypp)
{
	return fy_expr_stack_peek(&fypp->operands);
}

static inline FY_UNUSED struct fy_path_expr *
pop_operand(struct fy_path_parser *fypp)
{
	return fy_expr_stack_pop(&fypp->operands);
}

#define PREFIX	0
#define INFIX	1
#define SUFFIX	2

int fy_token_type_operator_placement(enum fy_token_type type)
{
	switch (type) {
	case FYTT_PE_SLASH:	/* SLASH is special at the start of the expression */
	case FYTT_PE_COMMA:
	case FYTT_PE_BARBAR:
	case FYTT_PE_AMPAMP:
	case FYTT_PE_EQEQ:
	case FYTT_PE_NOTEQ:
	case FYTT_PE_LT:
	case FYTT_PE_GT:
	case FYTT_PE_LTE:
	case FYTT_PE_GTE:
	case FYTT_SE_PLUS:
	case FYTT_SE_MINUS:
	case FYTT_SE_MULT:
	case FYTT_SE_DIV:
		return INFIX;
	case FYTT_PE_SCALAR_FILTER:
	case FYTT_PE_COLLECTION_FILTER:
	case FYTT_PE_SEQ_FILTER:
	case FYTT_PE_MAP_FILTER:
	case FYTT_PE_UNIQUE_FILTER:
		return SUFFIX;
	case FYTT_PE_SIBLING:
		return PREFIX;
	default:
		break;
	}
	return -1;
}

const struct fy_mark *fy_path_expr_start_mark(struct fy_path_expr *expr)
{
	if (!expr)
		return NULL;

	return fy_token_start_mark(expr->fyt);
}

const struct fy_mark *fy_path_expr_end_mark(struct fy_path_expr *expr)
{
	if (!expr)
		return NULL;

	return fy_token_end_mark(expr->fyt);
}

struct fy_token *
expr_to_token_mark(struct fy_path_expr *expr, struct fy_input *fyi)
{
	const struct fy_mark *ms, *me;
	struct fy_atom handle;

	if (!expr || !fyi)
		return NULL;

	ms = fy_path_expr_start_mark(expr);
	assert(ms);
	me = fy_path_expr_end_mark(expr);
	assert(me);

	memset(&handle, 0, sizeof(handle));
	handle.start_mark = *ms;
	handle.end_mark = *me;
	handle.fyi = fyi;
	handle.style = FYAS_PLAIN;
	handle.chomp = FYAC_CLIP;

	return fy_token_create(FYTT_INPUT_MARKER, &handle);
}

struct fy_token *
expr_lr_to_token_mark(struct fy_path_expr *exprl, struct fy_path_expr *exprr, struct fy_input *fyi)
{
	const struct fy_mark *ms, *me;
	struct fy_atom handle;

	if (!exprl || !exprr || !fyi)
		return NULL;

	ms = fy_path_expr_start_mark(exprl);
	assert(ms);
	me = fy_path_expr_end_mark(exprr);
	assert(me);

	memset(&handle, 0, sizeof(handle));
	handle.start_mark = *ms;
	handle.end_mark = *me;
	handle.fyi = fyi;
	handle.style = FYAS_PLAIN;
	handle.chomp = FYAC_CLIP;

	return fy_token_create(FYTT_INPUT_MARKER, &handle);
}

int
fy_path_expr_order(struct fy_path_expr *expr1, struct fy_path_expr *expr2)
{
	const struct fy_mark *m1 = NULL, *m2 = NULL;

	if (expr1)
		m1 = fy_path_expr_start_mark(expr1);

	if (expr2)
		m2 = fy_path_expr_start_mark(expr2);

	if (m1 == m2)
		return 0;

	if (!m1)
		return -1;

	if (!m2)
		return 1;

	return m1->input_pos == m2->input_pos ? 0 :
	       m1->input_pos  < m2->input_pos ? -1 : 1;
}

int push_operand_lr(struct fy_path_parser *fypp,
		    enum fy_path_expr_type type,
		    struct fy_path_expr *exprl, struct fy_path_expr *exprr,
		    bool optimize)
{
	struct fy_reader *fyr;
	struct fy_path_expr *expr = NULL, *exprt;
	const struct fy_mark *ms = NULL, *me = NULL;
	struct fy_atom handle;
	int ret;

	optimize = false;
	assert(exprl || exprr);

	fyr = &fypp->reader;

#if 0
	fyr_notice(fyr, ">>> %s <%s> l=<%s> r=<%s>\n", __func__,
			fy_path_expr_type_txt[type],
			exprl ?fy_path_expr_type_txt[exprl->type] : "NULL",
			exprr ?fy_path_expr_type_txt[exprr->type] : "NULL");
#endif

	expr = fy_path_expr_alloc_recycle(fypp);
	fyr_error_check(fyr, expr, err_out,
			"fy_path_expr_alloc_recycle() failed\n");

	expr->type = type;
	expr->fyt = NULL;

	if (exprl) {
		assert(exprl->fyt);
		ms = fy_token_start_mark(exprl->fyt);
		assert(ms);
	} else {
		ms = fy_token_start_mark(exprr->fyt);
		assert(ms);
	}

	if (exprr) {
		assert(exprr->fyt);
		me = fy_token_end_mark(exprr->fyt);
		assert(me);
	} else {
		me = fy_token_end_mark(exprr->fyt);
		assert(me);
	}

	assert(ms && me);

	memset(&handle, 0, sizeof(handle));
	handle.start_mark = *ms;
	handle.end_mark = *me;
	handle.fyi = fypp->fyi;
	handle.style = FYAS_PLAIN;
	handle.chomp = FYAC_CLIP;

	if (exprl) {
		if (type == exprl->type && fy_path_expr_type_is_mergeable(type)) {
			while ((exprt = fy_path_expr_list_pop(&exprl->children)) != NULL) {
				fy_path_expr_list_add_tail(&expr->children, exprt);
				exprt->parent = expr;
			}
			fy_path_expr_free_recycle(fypp, exprl);
		} else {
			fy_path_expr_list_add_tail(&expr->children, exprl);
			exprl->parent = expr;
		}
		exprl = NULL;
	}

	if (exprr) {
		if (type == exprr->type && fy_path_expr_type_is_mergeable(type)) {
			while ((exprt = fy_path_expr_list_pop(&exprr->children)) != NULL) {
				fy_path_expr_list_add_tail(&expr->children, exprt);
				exprt->parent = expr;
			}
			fy_path_expr_free_recycle(fypp, exprr);
		} else {
			fy_path_expr_list_add_tail(&expr->children, exprr);
			exprr->parent = expr;
		}
		exprr = NULL;
	}

	expr->fyt = fy_token_create(FYTT_INPUT_MARKER, &handle);
	fyr_error_check(fyr, expr->fyt, err_out,
			"expr_to_token_mark() failed\n");

	ret = push_operand(fypp, expr);
	fyr_error_check(fyr, !ret, err_out,
			"push_operand() failed\n");

#ifdef DEBUG_EXPR
	FYR_TOKEN_DIAG(fyr, expr->fyt,
		FYDF_NOTICE, FYEM_PARSE, "pushed operand lr");
#endif

	return 0;
err_out:
	fy_path_expr_free(expr);
	fy_path_expr_free(exprl);
	fy_path_expr_free(exprr);
	return -1;
}

enum fy_method_idx {
	fymi_test,
	fymi_sum,
	fymi_this,
	fymi_parent,
	fymi_root,
	fymi_any,
	fymi_all,
	fymi_select,
	fymi_key,
	fymi_value,
	fymi_index,
	fymi_null,
};

static struct fy_walk_result *
common_builtin_ref_exec(const struct fy_method *fym,
	struct fy_path_exec *fypx, int level,
	struct fy_path_expr *expr,
	struct fy_walk_result *input,
	struct fy_walk_result **args, int nargs);

static struct fy_walk_result *
common_builtin_collection_exec(const struct fy_method *fym,
	struct fy_path_exec *fypx, int level,
	struct fy_path_expr *expr,
	struct fy_walk_result *input,
	struct fy_walk_result **args, int nargs);


static struct fy_walk_result *
test_exec(const struct fy_method *fym,
	  struct fy_path_exec *fypx, int level,
	  struct fy_path_expr *expr,
	  struct fy_walk_result *input,
	  struct fy_walk_result **args, int nargs);

static struct fy_walk_result *
sum_exec(const struct fy_method *fym,
	 struct fy_path_exec *fypx, int level,
	 struct fy_path_expr *expr,
	 struct fy_walk_result *input,
	 struct fy_walk_result **args, int nargs);

static const struct fy_method fy_methods[] = {
	[fymi_test] = {
		.name	= "test",
		.len    = 4,
		.mode	= fyem_scalar,
		.nargs	= 1,
		.exec	= test_exec,
	},
	[fymi_sum] = {
		.name	= "sum",
		.len    = 3,
		.mode	= fyem_scalar,
		.nargs	= 2,
		.exec	= sum_exec,
	},
	[fymi_this] = {
		.name	= "this",
		.len    = 4,
		.mode	= fyem_path,
		.nargs	= 0,
		.exec	= common_builtin_ref_exec,
	},
	[fymi_parent] = {
		.name	= "parent",
		.len    = 6,
		.mode	= fyem_path,
		.nargs	= 0,
		.exec	= common_builtin_ref_exec,
	},
	[fymi_root] = {
		.name	= "root",
		.len    = 4,
		.mode	= fyem_path,
		.nargs	= 0,
		.exec	= common_builtin_ref_exec,
	},
	[fymi_any] = {
		.name	= "any",
		.len    = 3,
		.mode	= fyem_path,
		.nargs	= 1,
		.exec	= common_builtin_collection_exec,
	},
	[fymi_all] = {
		.name	= "all",
		.len    = 3,
		.mode	= fyem_path,
		.nargs	= 1,
		.exec	= common_builtin_collection_exec,
	},
	[fymi_select] = {
		.name	= "select",
		.len    = 6,
		.mode	= fyem_path,
		.nargs	= 1,
		.exec	= common_builtin_collection_exec,
	},
	[fymi_key] = {
		.name	= "key",
		.len    = 3,
		.mode	= fyem_path,
		.nargs	= 1,
		.exec	= common_builtin_ref_exec,
	},
	[fymi_value] = {
		.name	= "value",
		.len    = 5,
		.mode	= fyem_path,
		.nargs	= 1,
		.exec	= common_builtin_ref_exec,
	},
	[fymi_index] = {
		.name	= "index",
		.len    = 5,
		.mode	= fyem_path,
		.nargs	= 1,
		.exec	= common_builtin_ref_exec,
	},
	[fymi_null] = {
		.name	= "null",
		.len    = 4,
		.mode	= fyem_path,
		.nargs	= 0,
		.exec	= common_builtin_ref_exec,
	},
};

static inline int fy_method_to_builtin_idx(const struct fy_method *fym)
{
	if (!fym || fym < fy_methods || fym >= &fy_methods[ARRAY_SIZE(fy_methods)])
		return -1;
	return fym - fy_methods;
}

static struct fy_walk_result *
common_builtin_ref_exec(const struct fy_method *fym,
	    struct fy_path_exec *fypx, int level,
            struct fy_path_expr *expr,
	    struct fy_walk_result *input,
	    struct fy_walk_result **args, int nargs)
{
	enum fy_method_idx midx;
	struct fy_walk_result *output = NULL;
	struct fy_walk_result *fwr, *fwrn;
	struct fy_node *fyn, *fynt;
	int i;

	if (!fypx || !input)
		goto out;

	i = fy_method_to_builtin_idx(fym);
	if (i < 0)
		goto out;
	midx = (enum fy_method_idx)i;

	switch (midx) {
	case fymi_key:
	case fymi_value:
		if (!args || nargs != 1 || !args[0] || args[0]->type != fwrt_string)
			goto out;
		break;
	case fymi_index:
		if (!args || nargs != 1 || !args[0] || args[0]->type != fwrt_number)
			goto out;
		break;
	case fymi_root:
	case fymi_parent:
	case fymi_this:
	case fymi_null:
		if (nargs != 0)
			goto out;
		break;
	default:
		goto out;
	}

	output = fy_path_exec_walk_result_create(fypx, fwrt_refs);
	assert(output);

	for (fwr = fy_walk_result_iter_start(input); fwr;
			fwr = fy_walk_result_iter_next(input, fwr)) {

		if (fwr->type != fwrt_node_ref || !fwr->fyn)
			continue;

		fynt = fwr->fyn;

		switch (midx) {
		case fymi_key:
		case fymi_value:
		case fymi_index:
		case fymi_this:
		case fymi_null:
			/* dereference alias */
			if (fy_node_is_alias(fynt)) {
				// fprintf(stderr, "%s: %s calling fy_node_alias_resolve_by_ypath()\n", __func__, fy_node_get_path_alloca(fyn));
				fynt = fy_node_alias_resolve_by_ypath(fynt);
			}
			break;
		default:
			break;
		}

		fyn = NULL;
		switch (midx) {
		case fymi_key:
			if (!fy_node_is_mapping(fynt))
				break;
			fyn = fy_node_mapping_lookup_key_by_string(fynt, args[0]->string, FY_NT);
			break;

		case fymi_value:
			if (!fy_node_is_mapping(fynt))
				break;
			fyn = fy_node_mapping_lookup_by_string(fynt, args[0]->string, FY_NT);
			break;

		case fymi_index:
			if (!fy_node_is_sequence(fynt))
				break;
			fyn = fy_node_sequence_get_by_index(fynt, (int)args[0]->number);
			break;

		case fymi_root:
			fyn = fy_document_root(fy_node_document(fynt));
			break;

		case fymi_parent:
			fyn = fy_node_get_parent(fynt);
			break;

		case fymi_null:
			if (!fy_node_is_mapping(fynt))
				break;
			fyn = fy_node_mapping_lookup_value_by_null_key(fynt);
			break;

		case fymi_this:
			fyn = fynt;
			break;

		default:
			break;
		}

		if (!fyn)
			continue;

		fwrn = fy_path_exec_walk_result_create(fypx, fwrt_node_ref, fyn);
		assert(fwrn);

		fy_walk_result_list_add_tail(&output->refs, fwrn);
	}

	/* output convert zero to NULL, singular to node_ref */
	if (output && output->type == fwrt_refs) {
		if (fy_walk_result_list_empty(&output->refs)) {
			fy_walk_result_free(output);
			output = NULL;
		} else if (fy_walk_result_list_is_singular(&output->refs)) {
			fwr = fy_walk_result_list_pop(&output->refs);
			assert(fwr);
			fy_walk_result_free(output);
			output = fwr;
		}
	}

out:
	fy_walk_result_free(input);
	if (args) {
		for (i = 0; i < nargs; i++)
			fy_walk_result_free(args[i]);
	}
	return output;
}

static struct fy_walk_result *
common_builtin_collection_exec(const struct fy_method *fym,
	 struct fy_path_exec *fypx, int level,
	 struct fy_path_expr *expr,
	 struct fy_walk_result *input,
	 struct fy_walk_result **args, int nargs)
{
	enum fy_method_idx midx;
	struct fy_walk_result *output = NULL;
	struct fy_walk_result *fwr, *fwrn, *fwrt;
	struct fy_path_expr *expr_arg;
	bool match, done;
	int input_count, match_count;
	int i;

	if (!fypx || !input)
		goto out;

	i = fy_method_to_builtin_idx(fym);
	if (i < 0)
		goto out;
	midx = (enum fy_method_idx)i;

	switch (midx) {
	case fymi_any:
	case fymi_all:
	case fymi_select:
		if (!args || nargs != 1 || !args[0])
			goto out;
		break;
	default:
		goto out;
	}

	expr_arg = fy_path_expr_list_head(&expr->children);
	assert(expr_arg);

	/* only handle inputs of node and refs */
	if (input->type != fwrt_node_ref && input->type != fwrt_refs)
		goto out;

	output = NULL;

	switch (midx) {
	case fymi_select:
		output = fy_path_exec_walk_result_create(fypx, fwrt_refs);
		assert(output);
		break;
	default:
		break;
	}


	done = false;
	match_count = input_count = 0;
	for (fwr = fy_walk_result_iter_start(input); fwr && !done; fwr = fy_walk_result_iter_next(input, fwr)) {

		input_count++;

		fwrt = fy_walk_result_clone(fwr);
		assert(fwrt);

		fwrn = fy_path_expr_execute(fypx, level + 1, expr_arg, fwrt, expr->type);

		match = fwrn != NULL;
		if (match)
			match_count++;

		switch (midx) {
		case fymi_any:
			/* on any match, we're done */
			if (match)
				done = true;
			break;

		case fymi_all:
			/* on any non match, we're done */
			if (!match)
				done = true;
			break;

		case fymi_select:
			/* select only works on node refs */
			if (fwr->type != fwrt_node_ref)
				break;
			if (match) {
				fwrt = fy_walk_result_clone(fwr);
				assert(fwrt);
				fy_walk_result_list_add_tail(&output->refs, fwrt);
			}
			break;

		default:
			break;
		}

		if (fwrn)
			fy_walk_result_free(fwrn);
	}

	switch (midx) {
	case fymi_any:
		if (input_count > 0 && match_count <= input_count) {
			output = input;
			input = NULL;
		}
		break;
	case fymi_all:
		if (input_count > 0 && match_count == input_count) {
			output = input;
			input = NULL;
		}
		break;

	default:
		break;
	}

	/* output convert zero to NULL, singular to node_ref */
	if (output && output->type == fwrt_refs) {
		if (fy_walk_result_list_empty(&output->refs)) {
			fy_walk_result_free(output);
			output = NULL;
		} else if (fy_walk_result_list_is_singular(&output->refs)) {
			fwr = fy_walk_result_list_pop(&output->refs);
			assert(fwr);
			fy_walk_result_free(output);
			output = fwr;
		}
	}

out:
	if (input)
		fy_walk_result_free(input);
	if (args) {
		for (i = 0; i < nargs; i++)
			fy_walk_result_free(args[i]);
	}

	return output;
}

static struct fy_walk_result *
test_exec(const struct fy_method *fym,
	  struct fy_path_exec *fypx, int level,
	  struct fy_path_expr *expr,
	  struct fy_walk_result *input,
	  struct fy_walk_result **args, int nargs)
{
	int i;
	struct fy_walk_result *output = NULL;

	if (!fypx || !args || nargs != 1)
		goto out;

	/* require a single number argument */
	if (!args[0] || args[0]->type != fwrt_number)
		goto out;

	/* reuse argument */
	output = args[0];
	args[0] = NULL;

	/* add 1 to the number */
	output->number += 1;

out:
	fy_walk_result_free(input);
	if (args) {
		for (i = 0; i < nargs; i++)
			fy_walk_result_free(args[i]);
	}
	return output;
}

static struct fy_walk_result *
sum_exec(const struct fy_method *fym,
	 struct fy_path_exec *fypx, int level,
         struct fy_path_expr *expr,
	 struct fy_walk_result *input,
	 struct fy_walk_result **args, int nargs)
{
	int i;
	struct fy_walk_result *output = NULL;

	if (!fypx || !args || nargs != 2)
		goto out;

	/* require two number argument */
	if (!args[0] || args[0]->type != fwrt_number ||
	    !args[1] || args[1]->type != fwrt_number)
		goto out;

	/* reuse argument */
	output = args[0];
	args[0] = NULL;

	/* add 1 to the number */
	output->number += args[1]->number;

out:
	fy_walk_result_free(input);
	if (args) {
		for (i = 0; i < nargs; i++)
			fy_walk_result_free(args[i]);
	}
	return output;
}

int evaluate_method(struct fy_path_parser *fypp, struct fy_path_expr *exprm,
		    struct fy_path_expr *exprl, struct fy_path_expr *exprr)
{
	struct fy_reader *fyr;
	struct fy_path_expr *exprt;
	struct fy_token *fyt;
	const char *text;
	size_t len;
	const struct fy_method *fym;
	unsigned int i, count;
	int ret;

	fyr = &fypp->reader;

#ifdef DEBUG_EXPR
	FYR_TOKEN_DIAG(fyr, exprm->fyt,
		FYDF_NOTICE, FYEM_PARSE, "evaluating method");
#endif

	text = fy_token_get_text(exprm->fyt, &len);
	fyr_error_check(fyr, text, err_out,
			"fy_token_get_text() failed\n");

	for (i = 0, fym = fy_methods; i < ARRAY_SIZE(fy_methods); i++, fym++) {
		if (fym->len == len && !memcmp(text, fym->name, len))
			break;
	}

	FYR_TOKEN_ERROR_CHECK(fyr, exprm->fyt, FYEM_PARSE,
			i < ARRAY_SIZE(fy_methods), err_out,
			"invalid method %.*s\n", (int)len, text);

	/* reuse exprm */
	count = 0;
	while ((exprt = fy_expr_stack_peek(&fypp->operands)) != NULL &&
		fy_path_expr_order(exprm, exprt) < 0) {

		exprt = fy_expr_stack_pop(&fypp->operands);
		assert(exprt);

#ifdef DEBUG_EXPR
		FYR_TOKEN_DIAG(fyr, exprt->fyt,
			FYDF_NOTICE, FYEM_PARSE, "poped argument %d", count);
#endif

		/* add in reverse order */
		fy_path_expr_list_add(&exprm->children, exprt);
		exprt->parent = exprm;
		count++;

	}

	if (exprr) {
		fyt = expr_lr_to_token_mark(exprm, exprr, fypp->fyi);
		fyr_error_check(fyr, fyt, err_out,
				"expr_lr_to_token_mark() failed\n");

		fy_token_unref(exprm->fyt);
		exprm->fyt = fyt;
	}

	FYR_TOKEN_ERROR_CHECK(fyr, exprm->fyt, FYEM_PARSE,
			fym->nargs == count, err_out,
			"too %s argument for method %s, expected %d, got %d\n",
			fym->nargs < count ? "many" : "few",
			fym->name, fym->nargs, count);

	exprm->fym = fym;

	if (exprl)
		fy_path_expr_free_recycle(fypp, exprl);
	if (exprr)
		fy_path_expr_free_recycle(fypp, exprr);

	/* and push as an operand */
	ret = push_operand(fypp, exprm);
	fyr_error_check(fyr, !ret, err_out,
			"push_operand() failed\n");

#ifdef DEBUG_EXPR
	FYR_TOKEN_DIAG(fyr, exprm->fyt,
		FYDF_NOTICE, FYEM_PARSE, "pushed operand evaluate_method");
#endif
	return 0;

err_out:
	/* we don't need the parentheses operators */
	fy_path_expr_free_recycle(fypp, exprm);
	if (exprl)
		fy_path_expr_free_recycle(fypp, exprl);
	if (exprr)
		fy_path_expr_free_recycle(fypp, exprr);

	return -1;
}

int evaluate_new(struct fy_path_parser *fypp)
{
	struct fy_reader *fyr;
	struct fy_path_expr *expr = NULL, *expr_peek, *exprt;
	struct fy_path_expr *exprl = NULL, *exprr = NULL, *chain = NULL, *exprm = NULL;
	struct fy_path_expr *parent = NULL;
	struct fy_token *fyt;
	enum fy_path_expr_type type, etype;
	int ret;

	fyr = &fypp->reader;

	expr = fy_expr_stack_pop(&fypp->operators);
	fyr_error_check(fyr, expr, err_out,
			"pop_operator() failed to find token operator to evaluate\n");

	assert(expr->fyt);

#ifdef DEBUG_EXPR
	FYR_TOKEN_DIAG(fyr, expr->fyt,
		FYDF_NOTICE, FYEM_PARSE, "poped operator expression");
#endif

	exprl = NULL;
	exprr = NULL;
	type = expr->type;
	switch (type) {

	case fpet_chain:

		/* dump_operand_stack(fypp); */
		/* dump_operator_stack(fypp); */

		/* peek the next operator */
		expr_peek = fy_expr_stack_peek(&fypp->operators);

		/* pop the top in either case */
		exprr = fy_expr_stack_pop(&fypp->operands);
		if (!exprr) {
			// fyr_notice(fyr, "ROOT value (with no arguments)\n");

			/* conver to root and push to operands */
			expr->type = fpet_root;

			ret = push_operand(fypp, expr);
			fyr_error_check(fyr, !ret, err_out,
					"push_operand() failed\n");
			return 0;
		}

#ifdef DEBUG_EXPR
		FYR_TOKEN_DIAG(fyr, exprr->fyt,
			FYDF_NOTICE, FYEM_PARSE, "exprr");
#endif

		/* expression is to the left, that means it's a root chain */
		if (fy_path_expr_order(expr, exprr) < 0 &&
		    (!(exprl = fy_expr_stack_peek(&fypp->operands)) ||
		     (expr_peek && fy_path_expr_order(exprl, expr_peek) <= 0))) {

			// fyr_notice(fyr, "ROOT operator (with arguments)\n");

			exprl = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, exprl, err_out,
					"fy_path_expr_alloc_recycle() failed\n");
			exprl->type = fpet_root;

			/* move token to the root */
			exprl->fyt = expr->fyt;
			expr->fyt = NULL;

		} else if (!(exprl = fy_expr_stack_pop(&fypp->operands))) {

			// fyr_notice(fyr, "COLLECTION operator\n");

			exprl = exprr;

			exprr = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, exprr, err_out,
					"fy_path_expr_alloc_recycle() failed\n");
			exprr->type = fpet_filter_collection;

			/* move token to the filter collection */
			exprr->fyt = expr->fyt;
			expr->fyt = NULL;

		} else {
			assert(exprr && exprl);

			// fyr_notice(fyr, "CHAIN operator\n");
		}

		/* we don't need the chain operator now */
		fy_path_expr_free_recycle(fypp, expr);
		expr = NULL;

		ret = push_operand_lr(fypp, fpet_chain, exprl, exprr, true);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand_lr() failed\n");
		return 0;

	case fpet_multi:
	case fpet_logical_or:
	case fpet_logical_and:

	case fpet_eq:
	case fpet_neq:
	case fpet_lt:
	case fpet_gt:
	case fpet_lte:
	case fpet_gte:

	case fpet_plus:
	case fpet_minus:
	case fpet_mult:
	case fpet_div:

		exprl = NULL;
		exprr = NULL;
		exprt = fy_expr_stack_peek(&fypp->operators);

		// fyr_error(fyr, "mode=%s top-mode=%s\n",
		//		fy_expr_mode_txt[fypp->expr_mode],
		//		exprt ? fy_expr_mode_txt[exprt->expr_mode] : "");

#if 0
		exprr = fy_expr_stack_peek(&fypp->operands);
		if (exprr && exprt && fy_path_expr_order(exprr, exprt) <= 0)
			exprr = NULL;
		else
#endif
			exprr = fy_expr_stack_pop(&fypp->operands);
		fyr_error_check(fyr, exprr, err_out,
				"fy_expr_stack_pop() failed for exprr\n");

#if 0
		exprl = fy_expr_stack_peek_at(&fypp->operands, 1);
		if (exprl && exprt && fy_path_expr_order(exprl, exprt) <= 0)
			exprl = NULL;
		else
#endif
			exprl = fy_expr_stack_pop(&fypp->operands);
		fyr_error_check(fyr, exprl, err_out,
				"fy_expr_stack_pop() failed for exprl\n");

		/* we don't need the operator now */
		fy_path_expr_free_recycle(fypp, expr);
		expr = NULL;

		ret = push_operand_lr(fypp, type, exprl, exprr, true);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand_lr() failed\n");

		break;

	case fpet_filter_collection:
	case fpet_filter_scalar:
	case fpet_filter_sequence:
	case fpet_filter_mapping:
	case fpet_filter_unique:

		exprl = fy_expr_stack_pop(&fypp->operands);
		FYR_TOKEN_ERROR_CHECK(fyr, expr->fyt, FYEM_PARSE,
				exprl, err_out,
				"filter operator without argument");

		exprr = fy_path_expr_alloc_recycle(fypp);
		fyr_error_check(fyr, exprr, err_out,
				"fy_path_expr_alloc_recycle() failed\n");
		exprr->type = type;

		/* move token to the filter collection */
		exprr->fyt = expr->fyt;
		expr->fyt = NULL;

		/* we don't need the operator now */
		fy_path_expr_free_recycle(fypp, expr);
		expr = NULL;

		/* push as a chain */
		ret = push_operand_lr(fypp, fpet_chain, exprl, exprr, true);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand_lr() failed\n");

		break;

	case fpet_lparen:
		abort();
		assert(0);

#ifdef DEBUG_EXPR
		FYR_TOKEN_DIAG(fyr, expr->fyt,
			FYDF_NOTICE, FYEM_PARSE, "(");
#endif

		return 0;

	case fpet_arg_separator:

		/* separator is right hand side of the expression now */
		exprr = expr;
		expr = NULL;

		/* evaluate until we hit a match to the rparen */
		exprl = fy_expr_stack_peek(&fypp->operators);

		if (!fy_path_expr_type_is_lparen(exprl->type)) {
			ret = evaluate_new(fypp);
			if (ret)
				goto err_out;
		}
		exprl = NULL;

		fy_path_expr_free_recycle(fypp, exprr);
		exprr = NULL;

		break;

	case fpet_rparen:

		/* rparen is right hand side of the expression now */
		exprr = expr;
		expr = NULL;

		/* evaluate until we hit a match to the rparen */
		while ((exprl = fy_expr_stack_peek(&fypp->operators)) != NULL) {

			if (fy_path_expr_type_is_lparen(exprl->type))
				break;

			ret = evaluate_new(fypp);
			if (ret)
				goto err_out;
		}

		FYR_TOKEN_ERROR_CHECK(fyr, exprr->fyt, FYEM_PARSE,
				exprl, err_out,
				"missing matching left parentheses");

		exprl = fy_expr_stack_pop(&fypp->operators);
		assert(exprl);

		exprt = fy_expr_stack_peek(&fypp->operands);

		etype = exprl->expr_mode == fyem_scalar ? fpet_scalar_expr : fpet_path_expr;

		/* already is an expression, reuse */
		if (exprt && exprt->type == etype) {

			fyt = expr_lr_to_token_mark(exprl, exprr, fypp->fyi);
			fyr_error_check(fyr, fyt, err_out,
					"expr_lr_to_token_mark() failed\n");
			fy_token_unref(exprt->fyt);
			exprt->fyt = fyt;
			exprt->expr_mode = exprl->expr_mode;

			/* we don't need the parentheses operators */
			fy_path_expr_free_recycle(fypp, exprl);
			exprl = NULL;
			fy_path_expr_free_recycle(fypp, exprr);
			exprr = NULL;

			return 0;
		}

		/* if it's method, evaluate */
		exprm = fy_expr_stack_peek(&fypp->operators);
		if (exprm && exprm->type == fpet_method) {

			exprm = fy_expr_stack_pop(&fypp->operators);
			assert(exprm);

			return evaluate_method(fypp, exprm, exprl, exprr);
		}

		expr = fy_path_expr_alloc_recycle(fypp);
		fyr_error_check(fyr, expr, err_out,
				"fy_path_expr_alloc_recycle() failed\n");
		expr->type = etype;
		expr->expr_mode = exprl->expr_mode;

		expr->fyt = expr_lr_to_token_mark(exprl, exprr, fypp->fyi);

		exprt = fy_expr_stack_pop(&fypp->operands);

		FYR_TOKEN_ERROR_CHECK(fyr, exprr->fyt, FYEM_PARSE,
				exprt, err_out,
				"empty expression in parentheses");

		fy_path_expr_list_add_tail(&expr->children, exprt);
		exprt->parent = expr;

		/* pop all operands that after exprl */
		while ((exprt = fy_expr_stack_peek(&fypp->operands)) != NULL &&
			fy_path_expr_order(exprt, exprl) >= 0) {
#ifdef DEBUG_EXPR
			FYR_TOKEN_DIAG(fyr, exprt->fyt,
				FYDF_NOTICE, FYEM_PARSE, "discarding argument");
#endif
			fy_path_expr_free_recycle(fypp, fy_expr_stack_pop(&fypp->operands));
		}

		if (exprl->expr_mode != fyem_none) {
			fypp->expr_mode = exprl->expr_mode;
#ifdef DEBUG_EXPR
			fyr_notice(fyr, "poping expr_mode %s\n", fy_expr_mode_txt[fypp->expr_mode]);
#endif
		}

		/* we don't need the parentheses operators */
		fy_path_expr_free_recycle(fypp, exprl);
		exprl = NULL;
		fy_path_expr_free_recycle(fypp, exprr);
		exprr = NULL;

		ret = push_operand(fypp, expr);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand() failed\n");

#ifdef DEBUG_EXPR
		FYR_TOKEN_DIAG(fyr, expr->fyt,
			FYDF_NOTICE, FYEM_PARSE, "pushed operand evaluate_new");
#endif
		return 0;

	case fpet_method:
		return evaluate_method(fypp, expr, NULL, NULL);

		/* shoud never */
	case fpet_scalar_expr:
	case fpet_path_expr:
		assert(0);
		abort();

	default:
		fyr_error(fyr, "Unknown expression %s\n", fy_path_expr_type_txt[expr->type]);
		goto err_out;
	}

	return 0;

err_out:

#ifdef DEBUG_EXPR
	if (expr)
		fy_path_expr_dump(expr, fypp->cfg.diag, FYET_NOTICE, 0, "expr:");
	if (exprl)
		fy_path_expr_dump(exprl, fypp->cfg.diag, FYET_NOTICE, 0, "exprl:");
	if (exprr)
		fy_path_expr_dump(exprr, fypp->cfg.diag, FYET_NOTICE, 0, "exprr:");
	if (chain)
		fy_path_expr_dump(chain, fypp->cfg.diag, FYET_NOTICE, 0, "chain:");
	if (parent)
		fy_path_expr_dump(parent, fypp->cfg.diag, FYET_NOTICE, 0, "parent:");

	fy_notice(fypp->cfg.diag, "operator stack\n");
	fy_expr_stack_dump(fypp->cfg.diag, &fypp->operators);
	fy_notice(fypp->cfg.diag, "operand stack\n");
	fy_expr_stack_dump(fypp->cfg.diag, &fypp->operands);
#endif

	fy_path_expr_free(expr);
	fy_path_expr_free(exprl);
	fy_path_expr_free(exprr);
	fy_path_expr_free(chain);
	fy_path_expr_free(parent);

	return -1;
}

int fy_path_check_expression_alias(struct fy_path_parser *fypp, struct fy_path_expr *expr)
{
	struct fy_reader *fyr;
	struct fy_path_expr *exprn;
	int rc;

	if (!expr)
		return 0;

	fyr = &fypp->reader;

	/* an alias with a parent.. must be the first one */
	if (expr->type == fpet_alias && expr->parent) {

		exprn = fy_path_expr_list_head(&expr->parent->children);

		/* an alias may only be the first of a path expression */
		FYR_TOKEN_ERROR_CHECK(fyr, expr->fyt, FYEM_PARSE,
				expr == exprn, err_out,
				"alias is not first in the path expresion");
	}

	for (exprn = fy_path_expr_list_head(&expr->children); exprn;
		exprn = fy_path_expr_next(&expr->children, exprn)) {

		rc = fy_path_check_expression_alias(fypp, exprn);
		if (rc)
			return rc;
	}

	return 0;

err_out:
	return -1;
}

/* check expression for validity */
int fy_path_check_expression(struct fy_path_parser *fypp, struct fy_path_expr *expr)
{
	int rc;

	rc = fy_path_check_expression_alias(fypp, expr);
	if (rc)
		return rc;

	return 0;
}

struct fy_path_expr *
fy_path_parse_expression(struct fy_path_parser *fypp)
{
	struct fy_reader *fyr;
	struct fy_token *fyt = NULL;
	enum fy_token_type fytt;
	struct fy_path_expr *expr, *expr_top, *exprt;
	enum fy_expr_mode old_scan_mode, prev_scan_mode;
	int ret, rc;
#ifdef DEBUG_EXPR
    char *dbg;
#endif

	/* the parser must be in the correct state */
	if (!fypp || fy_expr_stack_size(&fypp->operators) > 0 || fy_expr_stack_size(&fypp->operands) > 0)
		return NULL;

	fyr = &fypp->reader;

	/* find stream start */
	fyt = fy_path_scan_peek(fypp, NULL);
	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_PARSE,
			fyt && fyt->type == FYTT_STREAM_START, err_out,
			"no tokens available or start without stream start");

	/* remove stream start */
	fy_token_unref(fy_path_scan_remove(fypp, fyt));
	fyt = NULL;

	prev_scan_mode = fypp->expr_mode;

	while ((fyt = fy_path_scan_peek(fypp, NULL)) != NULL) {

		if (fyt->type == FYTT_STREAM_END)
			break;

#ifdef DEBUG_EXPR
        fy_token_debug_text_a(fyt, &dbg);
		FYR_TOKEN_DIAG(fyr, fyt, FYET_NOTICE, FYEM_PARSE, "next token %s", dbg);
#endif
		fytt = fyt->type;

		/* create an expression in either operator/operand case */
		expr = fy_path_expr_alloc_recycle(fypp);
		fyr_error_check(fyr, expr, err_out,
				"fy_path_expr_alloc_recycle() failed\n");

		expr->fyt = fy_path_scan_remove(fypp, fyt);
		/* this it the first attempt, it might not be the final one */
		expr->type = fy_map_token_to_path_expr_type(fyt->type, fypp->expr_mode);
		fyt = NULL;

#ifdef DEBUG_EXPR
		fy_path_expr_dump(expr, fypp->cfg.diag, FYET_NOTICE, 0, "-> expr");
#endif

		if (prev_scan_mode != fypp->expr_mode) {
#ifdef DEBUG_EXPR
			fyr_warning(fyr, "switched expr_mode %s -> %s\n",
					fy_expr_mode_txt[prev_scan_mode],
					fy_expr_mode_txt[fypp->expr_mode]);
#endif
			expr_top = fy_expr_stack_peek(&fypp->operators);

#ifdef DEBUG_EXPR
			if (expr_top)
				fy_path_expr_dump(expr_top, fypp->cfg.diag, FYET_NOTICE, 0, NULL);
#endif

			if (expr_top && fy_path_expr_type_is_lparen(expr_top->type) &&
					expr_top->expr_mode != fypp->expr_mode) {
#ifdef DEBUG_EXPR
				fyr_warning(fyr, "switched top lparen expr_mode %s -> %s\n",
						fy_expr_mode_txt[expr_top->expr_mode],
						fy_expr_mode_txt[fypp->expr_mode]);
				expr_top->expr_mode = fypp->expr_mode;
#endif
			}
		}

		prev_scan_mode = fypp->expr_mode;

		/* if it's an operand convert it to expression and push */
		if (fy_token_type_is_operand(fytt)) {

			ret = fy_expr_stack_push(&fypp->operands, expr);
			fyr_error_check(fyr, !ret, err_out, "push_operand() failed\n");
			expr = NULL;

			continue;
		}

		/* specials for SLASH */

		if (expr->fyt->type == FYTT_PE_SLASH) {

			/* try to get next token */
			fyt = fy_path_scan_peek(fypp, NULL);
			if (!fyt) {
				if (!fypp->stream_error) {
					(void)fy_path_fetch_tokens(fypp);
					fyt = fy_path_scan_peek(fypp, NULL);
				}
			}

			/* last token, it means it's a collection filter (or a root) */
			if (!fyt || fyt->type == FYTT_STREAM_END || fyt->type == FYTT_PE_RPAREN) {

				exprt = fy_expr_stack_peek(&fypp->operands);

				/* if no argument exists it's a root */
				if (!exprt) {
					expr->type = fpet_root;

					ret = fy_expr_stack_push(&fypp->operands, expr);
					fyr_error_check(fyr, !ret, err_out, "push_operand() failed\n");
					expr = NULL;
					continue;
				}

				expr->type = fpet_filter_collection;
			}
		}

#ifdef DEBUG_EXPR
		fy_notice(fypp->cfg.diag, "operator stack (before)\n");
		fy_expr_stack_dump(fypp->cfg.diag, &fypp->operators);
		fy_notice(fypp->cfg.diag, "operand stack (before)\n");
		fy_expr_stack_dump(fypp->cfg.diag, &fypp->operands);
#endif

		old_scan_mode = fypp->expr_mode;

		/* for rparen, need to push before */
		if (expr->type == fpet_rparen) {

			FYR_TOKEN_ERROR_CHECK(fyr, expr->fyt, FYEM_PARSE,
					fypp->paren_nest_level > 0, err_out,
					"Mismatched right parentheses");

			fypp->paren_nest_level--;

			ret = fy_expr_stack_push(&fypp->operators, expr);
			fyr_error_check(fyr, !ret, err_out, "push_operator() failed\n");
			expr = NULL;

			ret = evaluate_new(fypp);
			/* evaluate will print diagnostic on error */
			if (ret < 0)
				goto err_out;

		} else if (fy_path_expr_type_is_lparen(expr->type)) {

			expr->expr_mode = fypp->expr_mode;
			fypp->expr_mode = fyem_scalar;

			fypp->paren_nest_level++;

			/* push the operator */
			ret = fy_expr_stack_push(&fypp->operators, expr);
			fyr_error_check(fyr, !ret, err_out, "push_operator() failed\n");
			expr = NULL;

#ifdef DEBUG_EXPR
			if (old_scan_mode != fypp->expr_mode)
				fyr_notice(fyr, "expr_mode %s -> %s\n",
						fy_expr_mode_txt[old_scan_mode],
						fy_expr_mode_txt[fypp->expr_mode]);
#endif
		} else {
			switch (fypp->expr_mode) {
			case fyem_none:
				assert(0);	/* should never happen */
				break;

			case fyem_path:
				if (fy_path_expr_type_is_conditional(expr->type)) {
					/* switch to scalar mode */
					fypp->expr_mode = fyem_scalar;
					break;
				}
				break;
			case fyem_scalar:
				if (expr->type == fpet_root) {
					fypp->expr_mode = fyem_path;
					break;
				}

				/* div out of parentheses, it's a chain */
				if (expr->type == fpet_div && fypp->paren_nest_level == 0) {
					expr->type = fpet_chain;
					fypp->expr_mode = fyem_path;

					/* mode change means evaluate */
					ret = evaluate_new(fypp);
					/* evaluate will print diagnostic on error */
					if (ret < 0)
						goto err_out;

					break;
				}
				break;
			}

			if (old_scan_mode != fypp->expr_mode) {
#ifdef DEBUG_EXPR
				fyr_notice(fyr, "expr_mode %s -> %s\n",
						fy_expr_mode_txt[old_scan_mode],
						fy_expr_mode_txt[fypp->expr_mode]);
#endif
			}

			ret = -1;
			while ((expr_top = fy_expr_stack_peek(&fypp->operators)) != NULL &&
				fy_path_expr_type_prec(expr->type) <= fy_path_expr_type_prec(expr_top->type) &&
				!fy_path_expr_type_is_lparen(expr_top->type)) {

				ret = evaluate_new(fypp);
				/* evaluate will print diagnostic on error */
				if (ret < 0)
					goto err_out;
			}

			/* push the operator */
			ret = fy_expr_stack_push(&fypp->operators, expr);
			fyr_error_check(fyr, !ret, err_out, "push_operator() failed\n");
			expr = NULL;
		}

#ifdef DEBUG_EXPR
		fy_notice(fypp->cfg.diag, "operator stack (after)\n");
		fy_expr_stack_dump(fypp->cfg.diag, &fypp->operators);
		fy_notice(fypp->cfg.diag, "operand stack (after)\n");
		fy_expr_stack_dump(fypp->cfg.diag, &fypp->operands);
#endif

		prev_scan_mode = fypp->expr_mode;
	}

	if (fypp->stream_error)
		goto err_out;

	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_PARSE,
			fypp->stream_error || (fyt && fyt->type == FYTT_STREAM_END), err_out,
			"stream ended without STREAM_END");

	/* remove stream end */
	fy_token_unref(fy_path_scan_remove(fypp, fyt));
	fyt = NULL;

#if 0
	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_PARSE,
			fypp->paren_nest_level == 0, err_out,
			"Missing right parenthesis");
#endif

	/* drain */
	while ((expr_top = fy_expr_stack_peek(&fypp->operators)) != NULL &&
		!fy_path_expr_type_is_lparen(expr_top->type)) {

		ret = evaluate_new(fypp);
		/* evaluate will print diagnostic on error */
		if (ret < 0)
			goto err_out;

	}

#ifdef DEBUG_EXPR
	expr_top = fy_expr_stack_peek(&fypp->operators);
	if (expr_top)
		fy_path_expr_dump(expr_top, fypp->cfg.diag, FYET_NOTICE, 0, "operator top left");
#endif

	expr = fy_expr_stack_pop(&fypp->operands);

	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_PARSE,
			expr != NULL, err_out,
			"No operands left on operand stack");

	FYR_TOKEN_ERROR_CHECK(fyr, expr->fyt, FYEM_PARSE,
			fy_expr_stack_size(&fypp->operands) == 0, err_out,
			"Operand stack contains more than 1 value at end");

	/* check the expression for validity */
	rc = fy_path_check_expression(fypp, expr);
	if (rc) {
		fy_path_expr_free(expr);
		expr = NULL;
	}

	return expr;

err_out:
#ifdef DEBUG_EXPR
	fy_notice(fypp->cfg.diag, "operator stack (error)\n");
	fy_expr_stack_dump(fypp->cfg.diag, &fypp->operators);
	fy_notice(fypp->cfg.diag, "operand stack (error)\n");
	fy_expr_stack_dump(fypp->cfg.diag, &fypp->operands);
#endif
	fypp->stream_error = true;
	return NULL;
}

static struct fy_node *
fy_path_expr_execute_single_result(struct fy_diag *diag, struct fy_path_expr *expr, struct fy_node *fyn)
{
	struct fy_token *fyt;
	struct fy_anchor *fya;
	const char *text;
	size_t len;

	assert(expr);

	switch (expr->type) {
	case fpet_root:
		return fyn->fyd->root;

	case fpet_this:
		if (fy_node_is_alias(fyn)) {
			// fprintf(stderr, "%s:%d %s calling fy_node_alias_resolve_by_ypath()\n", __func__, __LINE__, fy_node_get_path_alloca(fyn));
			fyn = fy_node_alias_resolve_by_ypath(fyn);
		}

		return fyn;

	case fpet_parent:
		return fy_node_get_parent(fyn);

	case fpet_alias:
		fyt = expr->fyt;
		assert(fyt);
		assert(fyt->type == FYTT_PE_ALIAS);

		text = fy_token_get_text(fyt, &len);
		if (!text || len < 1)
			break;

		if (*text == '*') {
			text++;
			len--;
		}
		fya = fy_document_lookup_anchor(fyn->fyd, text, len);
		if (!fya)
			break;
		return fya->fyn;

	case fpet_seq_index:
		fyt = expr->fyt;
		assert(fyt);
		assert(fyt->type == FYTT_PE_SEQ_INDEX);

		if (fy_node_is_alias(fyn)) {
			// fprintf(stderr, "%s:%d %s calling fy_node_alias_resolve_by_ypath()\n", __func__, __LINE__, fy_node_get_path_alloca(fyn));
			fyn = fy_node_alias_resolve_by_ypath(fyn);
		}

		/* only on sequence */
		if (!fy_node_is_sequence(fyn))
			break;

		return fy_node_sequence_get_by_index(fyn, fyt->seq_index.index);

	case fpet_map_key:
		fyt = expr->fyt;
		assert(fyt);
		assert(fyt->type == FYTT_PE_MAP_KEY);

		if (fy_node_is_alias(fyn)) {
			// fprintf(stderr, "%s:%d %s calling fy_node_alias_resolve_by_ypath()\n", __func__, __LINE__, fy_node_get_path_alloca(fyn));
			fyn = fy_node_alias_resolve_by_ypath(fyn);
		}

		if (!fy_node_is_mapping(fyn))
			break;

		if (!fyt->map_key.fyd) {
			/* simple key */
			text = fy_token_get_text(fyt, &len);
			if (!text || len < 1)
				break;
			return fy_node_mapping_lookup_value_by_simple_key(fyn, text, len);
		}

		return fy_node_mapping_lookup_value_by_key(fyn, fyt->map_key.fyd->root);

	case fpet_filter_scalar:
		if (!(fy_node_is_scalar(fyn) || fy_node_is_alias(fyn)))
			break;
		return fyn;

	case fpet_filter_collection:

		if (fy_node_is_alias(fyn)) {
			// fprintf(stderr, "%s:%d %s calling fy_node_alias_resolve_by_ypath()\n", __func__, __LINE__, fy_node_get_path_alloca(fyn));
			fyn = fy_node_alias_resolve_by_ypath(fyn);
		}

		if (!(fy_node_is_mapping(fyn) || fy_node_is_sequence(fyn)))
			break;
		return fyn;

	case fpet_filter_sequence:

		if (fy_node_is_alias(fyn)) {
			// fprintf(stderr, "%s:%d %s calling fy_node_alias_resolve_by_ypath()\n", __func__, __LINE__, fy_node_get_path_alloca(fyn));
			fyn = fy_node_alias_resolve_by_ypath(fyn);
		}

		if (!fy_node_is_sequence(fyn))
			break;
		return fyn;

	case fpet_filter_mapping:

		if (fy_node_is_alias(fyn)) {
			// fprintf(stderr, "%s:%d %s calling fy_node_alias_resolve_by_ypath()\n", __func__, __LINE__, fy_node_get_path_alloca(fyn));
			fyn = fy_node_alias_resolve_by_ypath(fyn);
		}

		if (!fy_node_is_mapping(fyn))
			break;
		return fyn;

	default:
		break;
	}

	return NULL;
}

static double
token_number(struct fy_token *fyt)
{
	const char *value;

	if (!fyt || fyt->type != FYTT_SCALAR || (value = fy_token_get_text0(fyt)) == NULL)
		return NAN;
	return strtod(value, NULL);
}

void fy_path_exec_cleanup(struct fy_path_exec *fypx)
{
	if (!fypx)
		return;

	fy_walk_result_free(fypx->result);
	fypx->result = NULL;
	fypx->fyn_start = NULL;
}

/* publicly exported methods */
struct fy_path_parser *fy_path_parser_create(const struct fy_path_parse_cfg *pcfg)
{
	struct fy_path_parser *fypp;

	fypp = malloc(sizeof(*fypp));
	if (!fypp)
		return NULL;
	fy_path_parser_setup(fypp, pcfg);
	return fypp;
}

void fy_path_parser_destroy(struct fy_path_parser *fypp)
{
	if (!fypp)
		return;
	fy_path_parser_cleanup(fypp);
	free(fypp);
}

int fy_path_parser_reset(struct fy_path_parser *fypp)
{
	if (!fypp)
		return -1;
	fy_path_parser_cleanup(fypp);
	return 0;
}

struct fy_path_expr *
fy_path_parse_expr_from_string(struct fy_path_parser *fypp,
			       const char *str, size_t len)
{
	struct fy_path_expr *expr = NULL;
	struct fy_input *fyi = NULL;
	int rc;

	if (!fypp || !str || !len)
		return NULL;

	fy_path_parser_reset(fypp);

	fyi = fy_input_from_data(str, len, NULL, false);
	if (!fyi) {
		fy_error(fypp->cfg.diag, "failed to create ypath input from %.*s\n",
				(int)len, str);
		goto err_out;
	}

	rc = fy_path_parser_open(fypp, fyi, NULL);
	if (rc) {
		fy_error(fypp->cfg.diag, "failed to open path parser input from %.*s\n",
				(int)len, str);
		goto err_out;
	}

	expr = fy_path_parse_expression(fypp);
	if (!expr) {
		fy_error(fypp->cfg.diag, "failed to parse path expression %.*s\n",
				(int)len, str);
		goto err_out;
	}

	fy_path_parser_close(fypp);

	fy_input_unref(fyi);

	return expr;

err_out:
	fy_path_expr_free(expr);
	fy_path_parser_close(fypp);
	fy_input_unref(fyi);
	return NULL;
}

struct fy_path_expr *
fy_path_expr_build_from_string(const struct fy_path_parse_cfg *pcfg,
			       const char *str, size_t len)
{
	struct fy_path_parser fypp_data, *fypp = &fypp_data;
	struct fy_path_expr *expr = NULL;

	if (!str)
		return NULL;

	fy_path_parser_setup(fypp, pcfg);
	expr = fy_path_parse_expr_from_string(fypp, str, len);
	fy_path_parser_cleanup(fypp);

	return expr;
}

struct fy_path_exec *fy_path_exec_create(const struct fy_path_exec_cfg *xcfg)
{
	struct fy_path_exec *fypx;

	fypx = malloc(sizeof(*fypx));
	if (!fypx)
		return NULL;

	memset(fypx, 0, sizeof(*fypx));
	if (xcfg)
		fypx->cfg = *xcfg;
	fypx->fwr_recycle = NULL;	/* initially no recycling list */
	fypx->refs = 1;

	fypx->supress_recycling = !!(fypx->cfg.flags & FYPXCF_DISABLE_RECYCLING) ||
		                  (getenv("FY_VALGRIND") &&
				   !getenv("FY_VALGRIND_RECYCLING"));
	return fypx;
}

struct fy_path_exec *fy_path_exec_create_on_document(struct fy_document *fyd)
{
	struct fy_path_exec_cfg xcfg_local, *xcfg = &xcfg_local;
	struct fy_path_exec *fypx;

	memset(xcfg, 0, sizeof(*xcfg));
	xcfg->diag = fyd ? fyd->diag : NULL;

	xcfg->flags = (fyd->parse_cfg.flags & FYPCF_DISABLE_RECYCLING) ?
			FYPXCF_DISABLE_RECYCLING : 0;

	fypx = fy_path_exec_create(xcfg);
	if (!fypx)
		return NULL;
	return fypx;
}

void fy_path_exec_destroy(struct fy_path_exec *fypx)
{
	if (!fypx)
		return;
	fy_path_exec_cleanup(fypx);
	free(fypx);
}

int fy_path_exec_reset(struct fy_path_exec *fypx)
{
	if (!fypx)
		return -1;
	fy_path_exec_cleanup(fypx);
	return 0;
}

struct fy_walk_result *fy_walk_result_simplify(struct fy_walk_result *fwr)
{
	struct fy_walk_result *fwr2;
#if 0
	struct fy_walk_result *fwrf;
	bool recursive;
#endif

	/* no fwr */
	if (!fwr)
		return NULL;

	/* non recursive */
	if (fwr->type != fwrt_refs)
		return fwr;

	/* refs, if empty, return NULL */
	if (fy_walk_result_list_empty(&fwr->refs)) {
		fy_walk_result_free(fwr);
		return NULL;
	}

	/* single element, switch it out */
	if (fy_walk_result_list_is_singular(&fwr->refs)) {
		fwr2 = fy_walk_result_list_pop(&fwr->refs);
		assert(fwr2);

		fy_walk_result_free(fwr);
		fwr = fwr2;
	}

	return fwr;
#if 0
	/* non recursive return immediately */
	if (fwr->type != fwrt_refs)
		return fwr;

	/* flatten if recursive */
	recursive = false;
	for (fwr2 = fy_walk_result_list_head(&fwr->refs); fwr2;
		fwr2 = fy_walk_result_next(&fwr->refs, fwr2)) {

		/* refs, recursive */
		if (fwr2->type == fwrt_refs) {
			recursive = true;
			break;
		}
	}

	if (!recursive)
		return fwr;

	fwrf = fy_path_exec_walk_result_create(fypx, fwrt_refs);
	assert(fwrf);

	fy_walk_result_flatten_internal(fwr, fwrf);

	fy_walk_result_free(fwr);
	return fwrf;
#endif

}

int fy_walk_result_all_children_recursive_internal(struct fy_path_exec *fypx, struct fy_node *fyn, struct fy_walk_result *output)
{
	struct fy_node *fyni;
	struct fy_walk_result *fwr;
	void *prevp;
	int ret;

	if (!fyn)
		return 0;

	assert(output);
	assert(output->type == fwrt_refs);

	/* this node */
	fwr = fy_path_exec_walk_result_create(fypx, fwrt_node_ref, fyn);
	if (!fwr)
		return -1;
	fy_walk_result_list_add_tail(&output->refs, fwr);

	if (fy_node_is_scalar(fyn))
		return 0;

	prevp = NULL;
	while ((fyni = fy_node_collection_iterate(fyn, &prevp)) != NULL) {
		ret = fy_walk_result_all_children_recursive_internal(fypx, fyni, output);
		if (ret)
			return ret;
	}

	return 0;
}

bool
fy_walk_result_compare_simple(struct fy_path_exec *fypx, enum fy_path_expr_type type,
			      struct fy_walk_result *fwrl, struct fy_walk_result *fwrr)
{
	struct fy_token *fyt;
	struct fy_walk_result *fwrt;
	const char *str;
	bool match;

	/* both NULL */
	if (!fwrl && !fwrr) {
		switch (type) {
		case fpet_eq:
			return true;
		default:
			break;
		}
		return false;
	}

	/* any NULL */
	if (!fwrl || !fwrr) {
		switch (type) {
		case fpet_neq:
			return true;
		default:
			break;
		}
		return false;
	}

	/* both are non NULL */

	/* none should be multiple */
	assert(fwrl->type != fwrt_refs && fwrr->type != fwrt_refs);

	/* both are the same type */
	if (fwrl->type == fwrr->type) {

		switch (fwrl->type) {
		case fwrt_none:
			abort();	/* should never happen */
			break;

		case fwrt_node_ref:
			switch (type) {
			case fpet_eq:
				/* simple and fast direct node comparison */
				if (fwrl->fyn == fwrr->fyn)
					return true;
				return fy_node_compare(fwrl->fyn, fwrr->fyn);
			case fpet_neq:
				/* simple and fast direct node comparison */
				if (fwrl->fyn != fwrr->fyn)
					return true;
				return !fy_node_compare(fwrl->fyn, fwrr->fyn);
			default:
				break;
			}
			break;

		case fwrt_refs:
			assert(0);	/* should not get here */
			break;

		case fwrt_doc:
			switch (type) {
			case fpet_eq:
			case fpet_neq:
				match = false;
				if (fwrl->fyd == fwrr->fyd)
					match = true;
				else if (!fwrl->fyd || !fwrr->fyd)
					match = false;
				else
					match = fy_node_compare(fwrl->fyd->root, fwrr->fyd->root);
				if (type == fpet_neq)
					match = !match;
				return match;
			default:
				break;
			}
			break;

		case fwrt_number:
			switch (type) {
			case fpet_eq:
				return fwrl->number == fwrr->number;
			case fpet_neq:
				return fwrl->number != fwrr->number;
			case fpet_lt:
				return fwrl->number < fwrr->number;
			case fpet_gt:
				return fwrl->number > fwrr->number;
			case fpet_lte:
				return fwrl->number <= fwrr->number;
			case fpet_gte:
				return fwrl->number >= fwrr->number;
			default:
				break;
			}
			break;

		case fwrt_string:
			switch (type) {
			case fpet_eq:
				return strcmp(fwrl->string, fwrr->string) == 0;
			case fpet_neq:
				return strcmp(fwrl->string, fwrr->string) != 0;
			case fpet_lt:
				return strcmp(fwrl->string, fwrr->string) < 0;
			case fpet_gt:
				return strcmp(fwrl->string, fwrr->string) > 0;
			case fpet_lte:
				return strcmp(fwrl->string, fwrr->string) <= 0;
			case fpet_gte:
				return strcmp(fwrl->string, fwrr->string) >= 0;
			default:
				break;
			}
			break;
		}
		return false;
	}

	/* only handle the node refs at the left */
	if (fwrr->type == fwrt_node_ref) {
		switch (type) {
		case fpet_lt:
			type = fpet_gte;
			break;
		case fpet_gt:
			type = fpet_lte;
			break;
		case fpet_lte:
			type = fpet_gt;
			break;
		case fpet_gte:
			type = fpet_lt;
			break;
		default:
			break;
		}

		/* swap left with right */
		return fy_walk_result_compare_simple(fypx, type, fwrr, fwrl);
	}

	switch (fwrl->type) {
	case fwrt_node_ref:

		/* non scalar mode, only returns true for non-eq */
		if (!fy_node_is_scalar(fwrl->fyn)) {
			/* XXX case of rhs being a document not handled */
			return type == fpet_neq;
		}

		fyt = fy_node_get_scalar_token(fwrl->fyn);
		assert(fyt);

		str = fy_token_get_text0(fyt);
		assert(str);

		fwrt = NULL;
		/* node ref against */
		switch (fwrr->type) {
		case fwrt_string:
			/* create a new temporary walk result */
			fwrt = fy_path_exec_walk_result_create(fypx, fwrt_string, str);
			assert(fwrt);

			break;

		case fwrt_number:
			/* if it's not a number return true only for non-eq */
			if (!fy_token_is_number(fyt))
				return type == fpet_neq;

			/* create a new temporary walk result */
			fwrt = fy_path_exec_walk_result_create(fypx, fwrt_number, strtod(str, NULL));
			assert(fwrt);
			break;

		default:
			break;
		}

		if (!fwrt)
			return false;

		match = fy_walk_result_compare_simple(fypx, type, fwrt, fwrr);

		/* free the temporary result */
		fy_walk_result_free(fwrt);

		return match;

	default:
		break;
	}

	return false;
}

struct fy_walk_result *
fy_walk_result_arithmetic_simple(struct fy_path_exec *fypx,
				 struct fy_path_expr *expr,
				 struct fy_path_expr *exprl, struct fy_walk_result *fwrl,
				 struct fy_path_expr *exprr, struct fy_walk_result *fwrr)
{
	struct fy_diag *diag;
	struct fy_walk_result *output = NULL;
	char *str;
	size_t len, len1, len2;

	if (!fwrl || !fwrr)
		goto out;

	diag = fypx->cfg.diag;

	/* node refs are not handled yet */
	if (fwrl->type == fwrt_node_ref || fwrr->type == fwrt_node_ref)
		goto out;

	/* same type */
	if (fwrl->type == fwrr->type) {

		switch (fwrl->type) {

		case fwrt_string:
			/* for strings, only concatenation */
			if (expr->type != fpet_plus)
				break;
			len1 = strlen(fwrl->string);
			len2 = strlen(fwrr->string);
			len = len1 + len2;
			str = malloc(len + 1);
			assert(str);
			memcpy(str, fwrl->string, len1);
			memcpy(str + len1, fwrr->string, len2);
			str[len] = '\0';

			free(fwrl->string);
			fwrl->string = str;

			/* reuse */
			output = fwrl;
			fwrl = NULL;

			break;

		case fwrt_number:
			/* reuse fwrl */
			output = fwrl;
			switch (expr->type) {
			case fpet_plus:
				output->number = fwrl->number + fwrr->number;
				break;
			case fpet_minus:
				output->number = fwrl->number - fwrr->number;
				break;
			case fpet_mult:
				output->number = fwrl->number * fwrr->number;
				break;
			case fpet_div:
				output->number = fwrr->number ? (fwrl->number / fwrr->number) : INFINITY;
				break;
			default:
				assert(0);
				break;
			}
			fwrl = NULL;
			break;

		default:
			fy_error(diag, "fwrl->type=%s\n", fy_walk_result_type_txt[fwrl->type]);
			assert(0);
			break;
		}
	}

out:
	fy_walk_result_free(fwrl);
	fy_walk_result_free(fwrr);
	return output;
}

struct fy_walk_result *
fy_walk_result_conditional_simple(struct fy_path_exec *fypx,
				  struct fy_path_expr *expr,
				  struct fy_path_expr *exprl, struct fy_walk_result *fwrl,
				  struct fy_path_expr *exprr, struct fy_walk_result *fwrr)
{
	bool match;

	match = fy_walk_result_compare_simple(fypx, expr->type, fwrl, fwrr);

	if (!match) {
		fy_walk_result_free(fwrl);
		fy_walk_result_free(fwrr);
		return NULL;
	}

	/* path expr, return left hand side result */
	fy_walk_result_free(fwrr);
	return fwrl;
}

struct fy_walk_result *
fy_walk_result_lhs_rhs(struct fy_path_exec *fypx,
		       struct fy_path_expr *expr,
		       struct fy_path_expr *exprl, struct fy_walk_result *fwrl,
		       struct fy_path_expr *exprr, struct fy_walk_result *fwrr)
{
	struct fy_walk_result *output = NULL, *fwr, *fwrrt, *fwrlt, *fwrlc, *fwrrc;
	struct fy_walk_result *outputl = NULL, *outputr = NULL;

	assert(expr);
	assert(exprl);
	assert(exprr);

	/* only supports those */
	if (!fy_path_expr_type_is_conditional(expr->type) &&
	    !fy_path_expr_type_is_arithmetic(expr->type))
		goto out;

	/* both NULL */
	if (!fwrl && !fwrr)
		goto out;

	/* any NULL */
	if (!fwrl || !fwrr) {
		if (expr->type == fpet_neq) {
			output = fwrl;
			fwrl = NULL;
		}
		goto out;
	}

	output = fy_path_exec_walk_result_create(fypx, fwrt_refs);
	assert(output);

	for (fwrlt = fy_walk_result_iter_start(fwrl); fwrlt;
			fwrlt = fy_walk_result_iter_next(fwrl, fwrlt)) {

		/* for recursive ones */
		if (fwrlt->type == fwrt_refs) {

			fwrlc = fy_walk_result_clone(fwrlt);
			assert(fwrlc);
			fwrrc = fy_walk_result_clone(fwrr);
			assert(fwrrc);

			outputl = fy_walk_result_lhs_rhs(fypx, expr, exprl, fwrlc, exprr, fwrrc);
			if (outputl)
				fy_walk_result_list_add_tail(&output->refs, outputl);
			else
				fy_walk_result_free(outputl);
			continue;
		}

		/* non-recursive case */
		for (fwrrt = fy_walk_result_iter_start(fwrr); fwrrt;
				fwrrt = fy_walk_result_iter_next(fwrr, fwrrt)) {

			/* for recursive ones */
			if (fwrrt->type == fwrt_refs) {

				fwrlc = fy_walk_result_clone(fwrlt);
				assert(fwrlc);
				fwrrc = fy_walk_result_clone(fwrrt);
				assert(fwrrc);

				outputr = fy_walk_result_lhs_rhs(fypx, expr, exprl, fwrlc, exprr, fwrrc);
				if (outputr)
					fy_walk_result_list_add_tail(&output->refs, outputr);
				else
					fy_walk_result_free(outputr);
				continue;
			}


			fwrlc = fy_walk_result_clone(fwrlt);
			assert(fwrlc);
			fwrrc = fy_walk_result_clone(fwrrt);
			assert(fwrrc);

			fwr = NULL;

			if (fy_path_expr_type_is_conditional(expr->type))
				fwr = fy_walk_result_conditional_simple(fypx, expr, exprl, fwrlc, exprr, fwrrc);
			else if (fy_path_expr_type_is_arithmetic(expr->type))
				fwr = fy_walk_result_arithmetic_simple(fypx, expr, exprl, fwrlc, exprr, fwrrc);
			else {
				assert(0);
			}

			fwrlc = NULL;
			fwrrc = NULL;

			if (fwr)
				fy_walk_result_list_add_tail(&output->refs, fwr);
		}
	}

out:
	fy_walk_result_free(fwrl);
	fy_walk_result_free(fwrr);

	return fy_walk_result_simplify(output);
}

struct fy_path_expr *
fy_scalar_walk_result_to_expr(struct fy_path_exec *fypx, struct fy_walk_result *fwr, enum fy_path_expr_type ptype)
{
	struct fy_input *fyit = NULL;
	struct fy_path_expr *exprt = NULL;
	struct fy_atom handle;
	bool collection_addressing;
	char *buf;
	int rc __FY_DEBUG_UNUSED__;

	exprt = NULL;

	if (!fwr)
		return NULL;

	collection_addressing = ptype == fpet_chain || ptype == fpet_multi;

	switch (fwr->type) {
	case fwrt_string:
		fyit = fy_input_from_malloc_data(fwr->string, FY_NT, &handle, true);
		assert(fyit);

		fwr->string = NULL;
		fy_walk_result_free(fwr);
		fwr = NULL;

		exprt = fy_path_expr_alloc();
		assert(exprt);
		if (collection_addressing) {
			exprt->type = fpet_map_key;
			exprt->fyt = fy_token_create(FYTT_PE_MAP_KEY, &handle, NULL);
			assert(exprt->fyt);
		} else {
			exprt->type = fpet_scalar;
			exprt->fyt = fy_token_create(FYTT_SCALAR, &handle, FYSS_PLAIN, NULL);
			assert(exprt->fyt);
		}
		break;

	case fwrt_number:

		rc = asprintf(&buf, "%d", (int)fwr->number);
		assert(rc != -1);

		fyit = fy_input_from_malloc_data(buf, FY_NT, &handle, true);
		assert(fyit);

		exprt = fy_path_expr_alloc();
		assert(exprt);
		if (collection_addressing) {
			exprt->type = fpet_seq_index;
			exprt->fyt = fy_token_create(FYTT_PE_SEQ_INDEX, &handle, (int)fwr->number);
			assert(exprt->fyt);
		} else {
			exprt->type = fpet_scalar;
			exprt->fyt = fy_token_create(FYTT_SCALAR, &handle, FYSS_PLAIN, NULL);
			assert(exprt->fyt);
		}

		break;

	default:
		break;
	}

	fy_walk_result_free(fwr);
	fy_input_unref(fyit);

	return exprt;
}

struct fy_walk_result *
fy_path_expr_execute(struct fy_path_exec *fypx, int level, struct fy_path_expr *expr,
		     struct fy_walk_result *input, enum fy_path_expr_type ptype)
{
	struct fy_diag *diag;
	struct fy_walk_result *fwr, *fwrn, *fwrt, *fwrtn;
	struct fy_walk_result *output = NULL, *input1, *output1, *input2, *output2;
	struct fy_path_expr *exprn, *exprl, *exprr;
	struct fy_node *fyn, *fynn, *fyni;
	struct fy_token *fyt;
	int start, end, count, i;
	bool match;
	struct fy_path_expr *exprt;
	unsigned int nargs;
	struct fy_walk_result **fwr_args;
	void *prevp;
	int rc __FY_DEBUG_UNUSED__;

	/* error */
	if (!fypx || !expr)
		goto out;

	diag = fypx->cfg.diag;

#ifdef DEBUG_EXPR
	if (input)
		fy_walk_result_dump(input, diag, FYET_NOTICE, level, "input %s\n", fy_path_expr_type_txt[expr->type]);
#endif

	/* recursive */
	if (input && input->type == fwrt_refs && !fy_path_expr_type_handles_refs(expr->type)) {

		output = fy_path_exec_walk_result_create(fypx, fwrt_refs);
		assert(output);

		while ((fwr = fy_walk_result_list_pop(&input->refs)) != NULL) {

			fwrn = fy_path_expr_execute(fypx, level + 1, expr, fwr, ptype);
			if (fwrn)
				fy_walk_result_list_add_tail(&output->refs, fwrn);
		}
		fy_walk_result_free(input);
		input = NULL;
		goto out;
	}


	/* single result case is common enough to optimize */
	if (fy_path_expr_type_is_single_result(expr->type)) {

		if (input && input->type == fwrt_node_ref) {

			fynn = fy_path_expr_execute_single_result(diag, expr, input->fyn);
			if (!fynn)
				goto out;

			fy_walk_result_clean(input);
			output = input;
			output->type = fwrt_node_ref;
			output->fyn = fynn;
			input = NULL;
		}

		goto out;
	}

	/* handle the remaining multi result cases */
	switch (expr->type) {

	case fpet_chain:

		if (!input)
			goto out;

		/* iterate over each chain item */
		output = input;
		input = NULL;
		for (exprn = fy_path_expr_list_head(&expr->children); exprn;
			exprn = fy_path_expr_next(&expr->children, exprn)) {

			output = fy_path_expr_execute(fypx, level + 1, exprn, output, expr->type);
			if (!output)
				break;
		}

		break;

	case fpet_multi:

		if (!input)
			goto out;

		/* allocate a refs output result */
		output = fy_path_exec_walk_result_create(fypx, fwrt_refs);
		assert(output);

		/* iterate over each multi item */
		for (exprn = fy_path_expr_list_head(&expr->children); exprn;
			exprn = fy_path_expr_next(&expr->children, exprn)) {

			input2 = fy_walk_result_clone(input);
			assert(input2);

			output2 = fy_path_expr_execute(fypx, level + 1, exprn, input2, expr->type);
			if (!output2)
				continue;

			fy_walk_result_list_add_tail(&output->refs, output2);
		}
		fy_walk_result_free(input);
		input = NULL;
		break;

	case fpet_every_child:

		if (!input)
			goto out;

		/* only valid for node ref */
		if (input->type != fwrt_node_ref)
			break;

		fyn = input->fyn;

		/* every scalar/alias is a single result (although it should not happen) */
		if (fy_node_is_scalar(fyn) || fy_node_is_alias(fyn)) {
			output = input;
			input = NULL;
			break;

		}

		/* re-use input for output root */
		fy_walk_result_clean(input);
		output = input;
		input = NULL;

		output->type = fwrt_refs;
		fy_walk_result_list_init(&output->refs);

		prevp = NULL;
		while ((fyni = fy_node_collection_iterate(fyn, &prevp)) != NULL) {
			fwr = fy_path_exec_walk_result_create(fypx, fwrt_node_ref, fyni);
			assert(fwr);

			fy_walk_result_list_add_tail(&output->refs, fwr);
		}

		break;

	case fpet_every_child_r:

		if (!input)
			goto out;

		/* only valid for node ref */
		if (input->type != fwrt_node_ref)
			break;

		fyn = input->fyn;

		/* re-use input for output root */
		fy_walk_result_clean(input);
		output = input;
		input = NULL;

		output->type = fwrt_refs;
		fy_walk_result_list_init(&output->refs);

		rc = fy_walk_result_all_children_recursive_internal(fypx, fyn, output);
		assert(!rc);

		break;

	case fpet_seq_slice:

		if (!input)
			goto out;

		/* only valid for node ref on a sequence */
		if (input->type != fwrt_node_ref || !fy_node_is_sequence(input->fyn)) {
			break;
		}
		fyn = input->fyn;

		fyt = expr->fyt;
		assert(fyt);
		assert(fyt->type == FYTT_PE_SEQ_SLICE);

		start = fyt->seq_slice.start_index;
		end = fyt->seq_slice.end_index;
		count = fy_node_sequence_item_count(fyn);

		/* don't handle negative slices yet */
		if (start < 0 || end < 1 || start >= end)
			break;

		if (count < end)
			end = count;

		/* re-use input for output root */
		fy_walk_result_clean(input);
		output = input;
		input = NULL;

		output->type = fwrt_refs;
		fy_walk_result_list_init(&output->refs);

		for (i = start; i < end; i++) {

			fynn = fy_node_sequence_get_by_index(fyn, i);
			if (!fynn)
				continue;

			fwr = fy_path_exec_walk_result_create(fypx, fwrt_node_ref, fynn);
			assert(fwr);

			fy_walk_result_list_add_tail(&output->refs, fwr);
		}

		break;

	case fpet_eq:
	case fpet_neq:
	case fpet_lt:
	case fpet_gt:
	case fpet_lte:
	case fpet_gte:
	case fpet_plus:
	case fpet_minus:
	case fpet_mult:
	case fpet_div:

		exprl = fy_path_expr_lhs(expr);
		assert(exprl);

		exprr = fy_path_expr_rhs(expr);
		assert(exprr);

		if (input) {
			input1 = fy_walk_result_clone(input);
			assert(input1);

			input2 = input;
			input = NULL;

		} else {
			input1 = NULL;
			input2 = NULL;
		}

		/* execute LHS and RHS */
		output1 = fy_path_expr_execute(fypx, level + 1, exprl, input1, expr->type);
		output2 = fy_path_expr_execute(fypx, level + 1, exprr, input2, expr->type);

		output = fy_walk_result_lhs_rhs(fypx, expr, exprl, output1, exprr, output2);

		break;

	case fpet_scalar:

		/* duck typing! */
		if (fy_token_is_number(expr->fyt)) {
			output = fy_path_exec_walk_result_create(fypx, fwrt_number, token_number(expr->fyt));
			assert(output);
		} else {
			output = fy_path_exec_walk_result_create(fypx, fwrt_string, fy_token_get_text0(expr->fyt));
			assert(output);
		}

		fy_walk_result_free(input);
		input = NULL;

		break;

	case fpet_logical_or:

		/* return the first that is not NULL */
		for (exprn = fy_path_expr_list_head(&expr->children); exprn;
			exprn = fy_path_expr_next(&expr->children, exprn)) {

			if (input) {
				input1 = fy_walk_result_clone(input);
				assert(input1);
			} else {
				input1 = NULL;
			}

			output = fy_path_expr_execute(fypx, level + 1, exprn, input1, expr->type);
			if (output)
				break;
		}
		break;

	case fpet_logical_and:
		output = NULL;

		/* return the last that was not NULL */
		for (exprn = fy_path_expr_list_head(&expr->children); exprn;
			exprn = fy_path_expr_next(&expr->children, exprn)) {

			if (input) {
				input1 = fy_walk_result_clone(input);
				assert(input1);
			} else {
				input1 = NULL;
			}

			output1 = fy_path_expr_execute(fypx, level + 1, exprn, input1, expr->type);
			if (output1) {
				fy_walk_result_free(output);
				output = output1;
			} else
				break;
		}
		break;

	case fpet_filter_unique:

		if (!input)
			goto out;

		/* flatten input */
		input = fy_walk_result_flatten(input);
		assert(input);	/* must work */

		/* for non refs, return input */
		if (input->type != fwrt_refs) {
			output = input;
			input = NULL;
			break;
		}

		/* remove duplicates filter */
		for (fwr = fy_walk_result_list_head(&input->refs); fwr;
				fwr = fy_walk_result_next(&input->refs, fwr)) {

			/* do not check recursively */
			if (fwr->type == fwrt_refs)
				continue;

			/* check the entries from this point forward */
			for (fwrt = fy_walk_result_next(&input->refs, fwr); fwrt; fwrt = fwrtn) {

				fwrtn = fy_walk_result_next(&input->refs, fwrt);

				/* do not check recursively (or the same result) */
				if (fwrt->type == fwrt_refs)
					continue;

				assert(fwrt != fwr);

				match = fy_walk_result_compare_simple(fypx, fpet_eq, fwr, fwrt);

				if (match) {
					fy_walk_result_list_del(&input->refs, fwrt);
					fy_walk_result_free(fwrt);
				}
			}
		}
		output = input;
		input = NULL;

		break;

	case fpet_scalar_expr:

		exprl = fy_path_expr_list_head(&expr->children);
		if (!exprl) {
			fy_warning(diag, "%s:%d\n", __FILE__, __LINE__);
			goto out;
		}

		output = fy_path_expr_execute(fypx, level + 1, exprl, NULL, ptype);
		if (!output) {
			fy_warning(diag, "%s:%d\n", __FILE__, __LINE__);
			goto out;
		}

		exprt = fy_scalar_walk_result_to_expr(fypx, output, ptype);
		output = NULL;
		if (!exprt) {
			fy_warning(diag, "%s:%d\n", __FILE__, __LINE__);
			break;
		}

		output = fy_path_expr_execute(fypx, level + 1, exprt, input, ptype);
		if (!output) {
			fy_warning(diag, "%s:%d\n", __FILE__, __LINE__);
		}
		input = NULL;

		fy_path_expr_free(exprt);
		break;

	case fpet_path_expr:
		exprl = fy_path_expr_list_head(&expr->children);
		if (!exprl)
			goto out;

		output = fy_path_expr_execute(fypx, level + 1, exprl, input, ptype);
		input = NULL;
		break;

	case fpet_method:

		assert(expr->fym);

		/* execute the arguments */
		nargs = expr->fym->nargs;
		if (nargs > 0) {
			fwr_args = FY_ALLOCA(sizeof(*fwr_args) * nargs);
			memset(fwr_args, 0, sizeof(*fwr_args) * nargs);
			for (i = 0, exprt = fy_path_expr_list_head(&expr->children); exprt;
					exprt = fy_path_expr_next(&expr->children, exprt), i++) {

				if (input) {
					input1 = fy_walk_result_clone(input);
					assert(input1);
				} else
					input1 = NULL;

				fwr_args[i] = fy_path_expr_execute(fypx, level + 1, exprt, input1, expr->type);
			}
		} else
			fwr_args = NULL;

		output = expr->fym->exec(expr->fym, fypx, level + 1, expr, input, fwr_args, nargs);
		input = NULL;

		break;

	default:
		fy_error(diag, "%s\n", fy_path_expr_type_txt[expr->type]);
		assert(0);
		break;
	}

out:
	fy_walk_result_free(input);
	output = fy_walk_result_simplify(output);

#ifdef DEBUG_EXPR
	if (output)
		fy_walk_result_dump(output, diag, FYET_NOTICE, level, "output %s\n", fy_path_expr_type_txt[expr->type]);
#endif
	return output;
}

static int fy_path_exec_execute_internal(struct fy_path_exec *fypx,
		struct fy_path_expr *expr, struct fy_node *fyn_start)
{
	struct fy_walk_result *fwr;

	if (!fypx || !expr || !fyn_start)
		return -1;

	fy_walk_result_free(fypx->result);
	fypx->result = NULL;

	fwr = fy_path_exec_walk_result_create(fypx, fwrt_node_ref, fyn_start);
	assert(fwr);

	fwr = fy_path_expr_execute(fypx, 0, expr, fwr, fpet_none);
	if (!fwr)
		return 0;

	/* flatten results */
	if (fwr->type == fwrt_refs) {
		fwr = fy_walk_result_flatten(fwr);
		if (!fwr)
			return -1;
	}
	fypx->result = fwr;

	return 0;
}

int fy_path_exec_execute(struct fy_path_exec *fypx, struct fy_path_expr *expr, struct fy_node *fyn_start)
{
	if (!fypx || !expr || !fyn_start)
		return -1;

	fypx->fyn_start = fyn_start;
	return fy_path_exec_execute_internal(fypx, expr, fypx->fyn_start);
}

struct fy_node *
fy_path_exec_results_iterate(struct fy_path_exec *fypx, void **prevp)
{
	struct fy_walk_result *fwr;

	if (!fypx || !prevp)
		return NULL;

	if (!fypx->result)
		return NULL;

	if (fypx->result->type != fwrt_refs) {
		fwr = fypx->result;

		if (fwr->type != fwrt_node_ref)
			return NULL;

		if (!*prevp) {
			*prevp = fwr;
			return fwr->fyn;
		}
		*prevp = NULL;
		return NULL;
	}

	/* loop over non node refs for now */
	do {
		if (!*prevp)
			fwr = fy_walk_result_list_head(&fypx->result->refs);
		else
			fwr = fy_walk_result_next(&fypx->result->refs, *prevp);
		*prevp = fwr;
	} while (fwr && fwr->type != fwrt_node_ref);

	return fwr ? fwr->fyn : NULL;
}

struct fy_walk_result *
fy_path_exec_take_results(struct fy_path_exec *fypx)
{
	struct fy_walk_result *fwr;

	if (!fypx || !fypx->result)
		return NULL;
	fwr = fypx->result;
	fypx->result = NULL;
	return fwr;
}

struct fy_walk_result *
fy_path_exec_walk_result_vcreate(struct fy_path_exec *fypx, enum fy_walk_result_type type, va_list ap)
{
	struct fy_walk_result_list *fwrl;

	if (!fypx)
		return NULL;

	fwrl = fy_path_exec_walk_result_rl(fypx);
	return fy_walk_result_vcreate_rl(fwrl, type, ap);
}

struct fy_walk_result *
fy_path_exec_walk_result_create(struct fy_path_exec *fypx, enum fy_walk_result_type type, ...)
{
	struct fy_walk_result_list *fwrl;
	struct fy_walk_result *fwr;
	va_list ap;

	if (!fypx)
		return NULL;

	fwrl = fy_path_exec_walk_result_rl(fypx);

	va_start(ap, type);
	fwr = fy_walk_result_vcreate_rl(fwrl, type, ap);
	va_end(ap);

	if (!fwr)
		return NULL;

	fwr->fypx = fy_path_exec_ref(fypx);

	return fwr;
}

void
fy_path_exec_walk_result_free(struct fy_path_exec *fypx, struct fy_walk_result *fwr)
{
	struct fy_walk_result_list *fwrl;

	fwrl = fypx ? fy_path_exec_walk_result_rl(fypx) : NULL;
	fy_walk_result_free_rl(fwrl, fwr);
}

int fy_document_setup_path_expr_data(struct fy_document *fyd)
{
	struct fy_path_parse_cfg pcfg_local, *pcfg = &pcfg_local;
	struct fy_path_expr_document_data *pxdd;

	if (!fyd || fyd->pxdd)
		return 0;

	pxdd = malloc(sizeof(*pxdd));
	if (!pxdd)
		goto err_no_mem;

	memset(pxdd, 0, sizeof(*pxdd));

	fy_walk_result_list_init(&pxdd->fwr_recycle);

	memset(pcfg, 0, sizeof(*pcfg));
	pcfg->diag = fyd->diag;
	pxdd->fypp = fy_path_parser_create(pcfg);
	if (!pxdd->fypp)
		goto err_no_fypp;

	fyd->pxdd = pxdd;

	return 0;

err_no_fypp:
	free(pxdd);
err_no_mem:
	return -1;
}

void fy_document_cleanup_path_expr_data(struct fy_document *fyd)
{
	struct fy_path_expr_document_data *pxdd;
	struct fy_walk_result *fwr;

	if (!fyd || !fyd->pxdd)
		return;

	pxdd = fyd->pxdd;

	fy_path_parser_destroy(pxdd->fypp);

	while ((fwr = fy_walk_result_list_pop(&pxdd->fwr_recycle)) != NULL)
		free(fwr);

	free(fyd->pxdd);
	fyd->pxdd = NULL;
}

int fy_node_setup_path_expr_data(struct fy_node *fyn)
{
	struct fy_path_expr_document_data *pxdd;
	struct fy_path_expr_node_data *pxnd;
	const char *text;
	size_t len;
	char *alloc = NULL;
	int rc;

	if (!fyn || fyn->pxnd)
		return 0;

	/* only on alias nodes */
	if (!fy_node_is_alias(fyn))
		return 0;

	/* a document must exist */
	if (!fyn->fyd)
		return -1;

	if (!fyn->fyd->pxdd) {
		rc = fy_document_setup_path_expr_data(fyn->fyd);
		if (rc)
			return rc;
	}
	pxdd = fyn->fyd->pxdd;
	assert(pxdd);

	pxnd = malloc(sizeof(*pxnd));
	if (!pxnd)
		goto err_no_mem;
	memset(pxnd, 0, sizeof(*pxnd));

	text = fy_token_get_text(fyn->scalar, &len);
	if (!text)
		goto err_no_text;

	if (!fy_is_first_alpha(*text)) {
		pxnd->fyi = fy_input_from_data(text, len, NULL, false);
		if (!pxnd->fyi)
			goto err_no_input;
	} else {
		alloc = malloc(len + 2);
		if (!alloc)
			goto err_no_input;
		alloc[0] = '*';
		memcpy(alloc + 1, text, len);
		alloc[len + 1] = '\0';

		pxnd->fyi = fy_input_from_malloc_data(alloc, len + 1, NULL, false);
		if (!pxnd->fyi)
			goto err_no_input;
	}

	fy_path_parser_reset(pxdd->fypp);

	rc = fy_path_parser_open(pxdd->fypp, pxnd->fyi, NULL);
	if (rc)
		goto err_no_open;

	pxnd->expr = fy_path_parse_expression(pxdd->fypp);
	if (!pxnd->expr)
		goto err_parse;

	fy_path_parser_close(pxdd->fypp);

	fyn->pxnd = pxnd;

	return 0;
err_parse:
	fy_path_parser_close(pxdd->fypp);
err_no_open:
	fy_input_unref(pxnd->fyi);
err_no_input:
	if (alloc)
		free(alloc);
err_no_text:
	free(pxnd);
err_no_mem:
	return -1;
}

void fy_node_cleanup_path_expr_data(struct fy_node *fyn)
{
	struct fy_path_expr_node_data *pxnd;

	if (!fyn || !fyn->pxnd)
		return;

	pxnd = fyn->pxnd;

	if (pxnd->expr)
		fy_path_expr_free(pxnd->expr);

	if (pxnd->fyi)
		fy_input_unref(pxnd->fyi);

	free(pxnd);
	fyn->pxnd = NULL;
}

struct fy_walk_result *
fy_node_alias_resolve_by_ypath_result(struct fy_node *fyn)
{
	struct fy_document *fyd;
	struct fy_path_expr_document_data *pxdd = NULL;
	struct fy_path_expr_node_data *pxnd = NULL;
	struct fy_walk_result *fwr;
	struct fy_anchor *fya;
	struct fy_path_exec *fypx = NULL;
	int rc;
	char* path;

	if (!fyn || !fy_node_is_alias(fyn))
		return NULL;

	fyd = fyn->fyd;
	if (!fyd)
		return NULL;

	/* simple */
	fya = fy_document_lookup_anchor_by_token(fyd, fyn->scalar);
	if (fya) {

		fwr = fy_path_exec_walk_result_create(fypx, fwrt_node_ref, fya->fyn);
		fyd_error_check(fyd, fwr, err_out,
				"fy_walk_result_alloc_rl() failed");

		return fwr;
	}

	/* ok, complex, setup the node data */
	rc = fy_node_setup_path_expr_data(fyn);
	fyd_error_check(fyd, !rc, err_out,
			"fy_node_setup_path_expr_data() failed");

	pxnd = fyn->pxnd;
	assert(pxnd);

	pxdd = fyd->pxdd;
	assert(pxdd);

	if (pxnd->traversals++ > 0) {
        fy_node_get_path_alloca(fyn, &path);
		FYD_NODE_ERROR(fyd, fyn, FYEM_DOC,
				"recursive reference detected at %s\n",
				path);
		pxnd->traversals--;
		return NULL;
	}

	fypx = fy_path_exec_create_on_document(fyd);
	fyd_error_check(fyd, !rc, err_out,
			"fy_path_exec_create_on_document() failed");

	fy_path_exec_set_result_recycle_list(fypx, &pxdd->fwr_recycle);

#if 0
	{
		struct fy_document *fyd_pe;
		const char *text;
		size_t len;

		text = fy_token_get_text(fyn->scalar, &len);
		if (text) {
			fyd_pe = fy_path_expr_to_document(pxnd->expr);
			if (fyd_pe) {
				fprintf(stderr, "%s: %.*s\n", __func__, (int)len, text);
				fy_document_default_emit_to_fp(fyd_pe, stderr);
				fy_document_destroy(fyd_pe);
			}
		}
	}
#endif

	// fprintf(stderr, "%s: %s 2\n", __func__, fy_node_get_path_alloca(fyn));

	/* execute, starting at this */
	rc = fy_path_exec_execute(fypx, pxnd->expr, fyn);
	fyd_error_check(fyd, !rc, err_out,
			"fy_path_exec_execute() failed");

	// fprintf(stderr, "%s: %s 3\n", __func__, fy_node_get_path_alloca(fyn));

	fwr = fy_path_exec_take_results(fypx);

	fy_path_exec_unref(fypx);

	pxnd->traversals--;

	if (!fwr)
		return NULL;

	// fprintf(stderr, "%s: %s 4\n", __func__, fy_node_get_path_alloca(fyn));

	return fwr;

err_out:
	if (pxnd)
		pxnd->traversals--;
	fy_path_exec_unref(fypx);	/* NULL OK */
	return NULL;
}

struct fy_node *fy_node_alias_resolve_by_ypath(struct fy_node *fyn)
{
	struct fy_anchor *fya;
	struct fy_walk_result *fwr;
	void *iterp;

	if (!fyn || !fy_node_is_alias(fyn))
		return NULL;

	/* simple and common enough to do it now */
	fya = fy_document_lookup_anchor_by_token(fyn->fyd, fyn->scalar);
	if (fya)
		return fya->fyn;

	fwr = fy_node_alias_resolve_by_ypath_result(fyn);
	if (!fwr)
		return NULL;

	iterp = NULL;
	fyn = fy_walk_result_node_iterate(fwr, &iterp);

	fy_walk_result_free(fwr);

	return fyn;
}

struct fy_walk_result *
fy_node_by_ypath_result(struct fy_node *fyn, const char *path, size_t len)
{
	struct fy_path_expr_document_data *pxdd;
	struct fy_document *fyd;
	struct fy_walk_result *fwr;
	struct fy_anchor *fya;
	struct fy_input *fyi;
	struct fy_path_expr *expr;
	struct fy_path_exec *fypx = NULL;
	int rc;

	if (!fyn || !path || !len)
		return NULL;

	fyd = fyn->fyd;
	if (!fyd)
		return NULL;

	if (len == FY_NT)
		len = strlen(path);

	/* simple */
	fya = fy_document_lookup_anchor(fyn->fyd, path, len);
	if (fya) {
		fwr = fy_path_exec_walk_result_create(fypx, fwrt_node_ref, fya->fyn);
		fyd_error_check(fyd, fwr, err_out,
				"fy_walk_result_alloc_rl() failed");
		return fwr;
	}

	/* ok, complex, setup the document data */
	rc = fy_document_setup_path_expr_data(fyd);
	fyd_error_check(fyd, !rc, err_setup,
			"fy_node_setup_path_expr_data() failed");

	pxdd = fyd->pxdd;
	assert(pxdd);

	fyi = fy_input_from_data(path, len, NULL, false);
	fyd_error_check(fyd, fyi, err_no_input,
			"fy_input_from_data() failed");

	fy_path_parser_reset(pxdd->fypp);

	rc = fy_path_parser_open(pxdd->fypp, fyi, NULL);
	fyd_error_check(fyd, !rc, err_no_open,
			"fy_path_parser_open() failed");

	expr = fy_path_parse_expression(pxdd->fypp);
	fyd_error_check(fyd, expr, err_parse,
			"fy_path_parse_expression() failed");

	fy_path_parser_close(pxdd->fypp);

	fypx = fy_path_exec_create_on_document(fyd);
	fyd_error_check(fyd, !rc, err_no_fypx,
			"fy_path_exec_create_on_document() failed");

	/* execute, starting at this */
	rc = fy_path_exec_execute(fypx, expr, fyn);
	fyd_error_check(fyd, !rc, err_exec,
			"fy_path_parse_expression() failed");

	fwr = fy_path_exec_take_results(fypx);

	fy_path_exec_unref(fypx);

	fy_path_expr_free(expr);
	fy_input_unref(fyi);

	return fwr;

err_exec:
	fy_path_expr_free(expr);
err_no_fypx:
	fy_path_exec_unref(fypx);
err_parse:
	fy_path_parser_close(pxdd->fypp);

err_no_open:
	fy_input_unref(fyi);

err_no_input:
err_setup:
err_out:
	return NULL;
}

struct fy_node *fy_node_by_ypath(struct fy_node *fyn, const char *path, size_t len)
{
	struct fy_walk_result *fwr;
	struct fy_anchor *fya;
	void *iterp;

	if (!fyn || !path || !len)
		return NULL;

	/* simple */
	fya = fy_document_lookup_anchor(fyn->fyd, path, len);
	if (fya)
		return fya->fyn;

	fwr = fy_node_by_ypath_result(fyn, path, len);
	if (!fwr)
		return NULL;

	iterp = NULL;
	fyn = fy_walk_result_node_iterate(fwr, &iterp);

	fy_walk_result_free(fwr);

	return fyn;
}
