/*
 * fy-emit.c - Internal YAML emitter methods
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
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
#include <limits.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#elif defined (_MSC_VER)
#define STDOUT_FILENO _fileno(stdin)
#endif
#include <ctype.h>
#include <errno.h>

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-emit.h"

/* fwd decl */
void fy_emit_write(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *str, int len);
void fy_emit_printf(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *fmt, ...)
		FY_ATTRIBUTE(format(printf, 3, 4));

static inline bool fy_emit_is_json_mode(const struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags;

	if (emit->force_json)
		return true;

	flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);
	return flags == FYECF_MODE_JSON || flags == FYECF_MODE_JSON_TP || flags == FYECF_MODE_JSON_ONELINE;
}

static inline bool fy_emit_is_flow_mode(const struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);

	return flags == FYECF_MODE_FLOW || flags == FYECF_MODE_FLOW_ONELINE;
}

static inline bool fy_emit_is_block_mode(const struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);

	return flags == FYECF_MODE_BLOCK || flags == FYECF_MODE_DEJSON || flags == FYECF_MODE_PRETTY;
}

static inline bool fy_emit_is_oneline(const struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);

	return flags == FYECF_MODE_FLOW_ONELINE || flags == FYECF_MODE_JSON_ONELINE;
}

static inline bool fy_emit_is_dejson_mode(const struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);

	return flags == FYECF_MODE_DEJSON;
}

static inline bool fy_emit_is_pretty_mode(const struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);

	return flags == FYECF_MODE_PRETTY;
}

static inline bool fy_emit_is_manual(const struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);

	return flags == FYECF_MODE_MANUAL;
}

static inline int fy_emit_indent(struct fy_emitter *emit)
{
	int indent;

	indent = (emit->cfg.flags & FYECF_INDENT(FYECF_INDENT_MASK)) >> FYECF_INDENT_SHIFT;
	return indent ? indent : 2;
}

static inline int fy_emit_width(struct fy_emitter *emit)
{
	int width;

	width = (emit->cfg.flags & FYECF_WIDTH(FYECF_WIDTH_MASK)) >> FYECF_WIDTH_SHIFT;
	if (width == 0)
		return 80;
	if (width == FYECF_WIDTH_MASK)
		return INT_MAX;
	return width;
}

static inline bool fy_emit_output_comments(struct fy_emitter *emit)
{
	return !!(emit->cfg.flags & FYECF_OUTPUT_COMMENTS);
}

static int fy_emit_node_check_json(struct fy_emitter *emit, struct fy_node *fyn)
{
	struct fy_document *fyd;
	struct fy_node *fyni;
	struct fy_node_pair *fynp, *fynpi;
	int ret;

	if (!fyn)
		return 0;

	fyd = fyn->fyd;

	switch (fyn->type) {
	case FYNT_SCALAR:
		FYD_TOKEN_ERROR_CHECK(fyd, fyn->scalar, FYEM_INTERNAL,
				!fy_node_is_alias(fyn), err_out,
				"aliases not allowed in JSON emit mode");
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {
			ret = fy_emit_node_check_json(emit, fyni);
			if (ret)
				return ret;
		}
		break;

	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn->mapping, fynp);

			ret = fy_emit_node_check_json(emit, fynp->key);
			if (ret)
				return ret;
			ret = fy_emit_node_check_json(emit, fynp->value);
			if (ret)
				return ret;
		}
		break;
	}
	return 0;
err_out:
	return -1;
}

static int fy_emit_node_check(struct fy_emitter *emit, struct fy_node *fyn)
{
	int ret;

	if (!fyn)
		return 0;

	if (fy_emit_is_json_mode(emit) && !emit->source_json) {
		ret = fy_emit_node_check_json(emit, fyn);
		if (ret)
			return ret;
	}

	return 0;
}

void fy_emit_node_internal(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent, bool is_key);
void fy_emit_scalar(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent, bool is_key);
void fy_emit_sequence(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent);
void fy_emit_mapping(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent);

void fy_emit_write(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *str, int len)
{
	int c, w;
	const char *m, *e;
	int outlen;

	if (!len)
		return;

	outlen = emit->cfg.output(emit, type, str, len, emit->cfg.userdata);
	if (outlen != len)
		emit->output_error = true;

	e = str + len;
	while ((c = fy_utf8_get(str, (e - str), &w)) >= 0) {

		/* special handling for MSDOS */
		if (c == '\r' && (e - str) > 1 && str[1] == '\n') {
			str += 2;
			emit->column = 0;
			emit->line++;
			continue;
		}

		/* regular line break */
		if (fy_is_lb_r_n(c)) {
			emit->column = 0;
			emit->line++;
			str += w;
			continue;
		}

		/* completely ignore ANSI color escape sequences */
		if (c == '\x1b' && (e - str) > 2 && str[1] == '[' &&
		    (m = memchr(str, 'm', e - str)) != NULL) {
			str = m + 1;
			continue;
		}

		emit->column++;
		str += w;
	}
}

void fy_emit_puts(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *str)
{
	fy_emit_write(emit, type, str, strlen(str));
}

void fy_emit_putc(struct fy_emitter *emit, enum fy_emitter_write_type type, int c)
{
	char buf[FY_UTF8_FORMAT_BUFMIN];

	fy_utf8_format(c, buf, fyue_none);
	fy_emit_puts(emit, type, buf);
}

void fy_emit_vprintf(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *fmt, va_list ap)
{
	char *str;
	int size;
	va_list ap2;

	va_copy(ap2, ap);

	size = vsnprintf(NULL, 0, fmt, ap);
	if (size < 0)
		return;

	str = FY_ALLOCA(size + 1);
	size = vsnprintf(str, size + 1, fmt, ap2);
	if (size < 0)
		return;

	fy_emit_write(emit, type, str, size);
}

void fy_emit_printf(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_emit_vprintf(emit, type, fmt, ap);
	va_end(ap);
}

void fy_emit_write_ws(struct fy_emitter *emit)
{
	fy_emit_putc(emit, fyewt_whitespace, ' ');
	emit->flags |= FYEF_WHITESPACE;
}

void fy_emit_write_indent(struct fy_emitter *emit, int indent)
{
	int len;
	char *ws;

	indent = indent > 0 ? indent : 0;

	if (!fy_emit_indentation(emit) || emit->column > indent ||
	    (emit->column == indent && !fy_emit_whitespace(emit)))
		fy_emit_putc(emit, fyewt_linebreak, '\n');

	if (emit->column < indent) {
		len = indent - emit->column;
		ws = FY_ALLOCA(len + 1);
		memset(ws, ' ', len);
		ws[len] = '\0';
		fy_emit_write(emit, fyewt_indent, ws, len);
	}

	emit->flags |= FYEF_WHITESPACE | FYEF_INDENTATION;
}

enum document_indicator {
	di_question_mark,
	di_colon,
	di_dash,
	di_left_bracket,
	di_right_bracket,
	di_left_brace,
	di_right_brace,
	di_comma,
	di_bar,
	di_greater,
	di_single_quote_start,
	di_single_quote_end,
	di_double_quote_start,
	di_double_quote_end,
	di_ambersand,
	di_star,
};

void fy_emit_write_indicator(struct fy_emitter *emit,
		enum document_indicator indicator,
		int flags, int indent,
		enum fy_emitter_write_type wtype)
{
	switch (indicator) {

	case di_question_mark:
		if (!fy_emit_whitespace(emit))
			fy_emit_write_ws(emit);
		fy_emit_putc(emit, wtype, '?');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_OPEN_ENDED);
		break;

	case di_colon:
		if (!(flags & DDNF_SIMPLE)) {
			if (!emit->flow_level && !fy_emit_is_oneline(emit))
				fy_emit_write_indent(emit, indent);
			if (!fy_emit_whitespace(emit))
				fy_emit_write_ws(emit);
		}
		fy_emit_putc(emit, wtype, ':');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_OPEN_ENDED);
		break;

	case di_dash:
		if (!fy_emit_whitespace(emit))
			fy_emit_write_ws(emit);
		fy_emit_putc(emit, wtype, '-');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_OPEN_ENDED);
		break;

	case di_left_bracket:
	case di_left_brace:
		emit->flow_level++;
		if (!fy_emit_whitespace(emit))
			fy_emit_write_ws(emit);
		fy_emit_putc(emit, wtype, indicator == di_left_bracket ? '[' : '{');
		emit->flags |= FYEF_WHITESPACE;
		emit->flags &= ~(FYEF_INDENTATION | FYEF_OPEN_ENDED);
		break;

	case di_right_bracket:
	case di_right_brace:
		emit->flow_level--;
		fy_emit_putc(emit, wtype, indicator == di_right_bracket ? ']' : '}');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION | FYEF_OPEN_ENDED);
		break;

	case di_comma:
		fy_emit_putc(emit, wtype, ',');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION | FYEF_OPEN_ENDED);
		break;

	case di_bar:
	case di_greater:
		if (!fy_emit_whitespace(emit))
			fy_emit_write_ws(emit);
		fy_emit_putc(emit, wtype, indicator == di_bar ? '|' : '>');
		emit->flags &= ~(FYEF_INDENTATION | FYEF_WHITESPACE | FYEF_OPEN_ENDED);
		break;

	case di_single_quote_start:
	case di_double_quote_start:
		if (!fy_emit_whitespace(emit))
			fy_emit_write_ws(emit);
		fy_emit_putc(emit, wtype, indicator == di_single_quote_start ? '\'' : '"');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION | FYEF_OPEN_ENDED);
		break;

	case di_single_quote_end:
	case di_double_quote_end:
		fy_emit_putc(emit, wtype, indicator == di_single_quote_end ? '\'' : '"');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION | FYEF_OPEN_ENDED);
		break;

	case di_ambersand:
		if (!fy_emit_whitespace(emit))
			fy_emit_write_ws(emit);
		fy_emit_putc(emit, wtype, '&');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION);
		break;

	case di_star:
		if (!fy_emit_whitespace(emit))
			fy_emit_write_ws(emit);
		fy_emit_putc(emit, wtype, '*');
		emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION);
		break;
	}
}

int fy_emit_increase_indent(struct fy_emitter *emit, int flags, int indent)
{
	if (indent < 0)
		return (flags & DDNF_FLOW) ? fy_emit_indent(emit) : 0;

	if (!(flags & DDNF_INDENTLESS))
		return indent + fy_emit_indent(emit);

	return indent;
}

void fy_emit_write_comment(struct fy_emitter *emit, int flags, int indent, const char *str, size_t len, struct fy_atom *handle)
{
	const char *s, *e, *sr;
	int c, w;
	bool breaks;

	if (!str || !len)
		return;

	if (len == (size_t)-1)
		len = strlen(str);

	if (!fy_emit_whitespace(emit))
		fy_emit_write_ws(emit);
	indent = emit->column;

	s = str;
	e = str + len;

	sr = s;	/* start of normal output run */
	breaks = false;
	while (s < e && (c = fy_utf8_get(s, e - s, &w)) > 0) {

		if (fy_is_lb_m(c, fy_atom_lb_mode(handle))) {

			/* output run */
			fy_emit_write(emit, fyewt_comment, sr, s - sr);
			sr = s + w;
			fy_emit_write_indent(emit, indent);
			emit->flags |= FYEF_INDENTATION;
			breaks = true;
		} else {

			if (breaks) {
				fy_emit_write(emit, fyewt_comment, sr, s - sr);
				sr = s;
				fy_emit_write_indent(emit, indent);
			}
			emit->flags &= ~FYEF_INDENTATION;
			breaks = false;
		}

		s += w;
	}

	/* dump what's remaining */
	fy_emit_write(emit, fyewt_comment, sr, s - sr);

	emit->flags |= (FYEF_WHITESPACE | FYEF_INDENTATION);
}

struct fy_atom *fy_emit_token_comment_handle(struct fy_emitter *emit, struct fy_token *fyt, enum fy_comment_placement placement)
{
	struct fy_atom *handle;

	handle = fy_token_comment_handle(fyt, placement, false);
	return handle && fy_atom_is_set(handle) ? handle : NULL;
}

void fy_emit_document_start_indicator(struct fy_emitter *emit)
{
	/* do not emit twice */
	if (emit->flags & FYEF_HAD_DOCUMENT_START)
		return;

	/* do not try to emit if it's json mode */
	if (fy_emit_is_json_mode(emit))
		goto no_doc_emit;

	/* output linebreak anyway */
	if (emit->column)
		fy_emit_putc(emit, fyewt_linebreak, '\n');

	/* stripping doc indicators, do not emit */
	if (emit->cfg.flags & FYECF_STRIP_DOC)
		goto no_doc_emit;

	/* ok, emit document start indicator */
	fy_emit_puts(emit, fyewt_document_indicator, "---");
	emit->flags &= ~FYEF_WHITESPACE;
	emit->flags |= FYEF_HAD_DOCUMENT_START;
	return;

no_doc_emit:
	emit->flags &= ~FYEF_HAD_DOCUMENT_START;
}

struct fy_token *fy_node_value_token(struct fy_node *fyn)
{
	struct fy_token *fyt;

	if (!fyn)
		return NULL;

	switch (fyn->type) {
	case FYNT_SCALAR:
		fyt = fyn->scalar;
		break;
	case FYNT_SEQUENCE:
		fyt = fyn->sequence_start;
		break;
	case FYNT_MAPPING:
		fyt = fyn->mapping_start;
		break;
	default:
		fyt = NULL;
		break;
	}

	return fyt;
}

bool fy_emit_token_has_comment(struct fy_emitter *emit, struct fy_token *fyt, enum fy_comment_placement placement)
{
	return fy_emit_token_comment_handle(emit, fyt, placement) ? true : false;
}

bool fy_emit_node_has_comment(struct fy_emitter *emit, struct fy_node *fyn, enum fy_comment_placement placement)
{
	return fy_emit_token_has_comment(emit, fy_node_value_token(fyn), placement);
}

void fy_emit_token_comment(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent,
			  enum fy_comment_placement placement)
{
	struct fy_atom *handle;
	char *text;
	const char *t;
	int len;

	handle = fy_emit_token_comment_handle(emit, fyt, placement);
	if (!handle)
		return;

	len = fy_atom_format_text_length(handle);
	if (len < 0)
		return;

	text = FY_ALLOCA(len + 1);

	if (placement == fycp_top || placement == fycp_bottom) {
		fy_emit_write_indent(emit, indent);
		emit->flags |= FYEF_WHITESPACE;
	}

	t = fy_atom_format_text(handle, text, len + 1);

	fy_emit_write_comment(emit, flags, indent, t, len, handle);

	emit->flags &= ~FYEF_INDENTATION;

	if (placement == fycp_top || placement == fycp_bottom) {
		fy_emit_write_indent(emit, indent);
		emit->flags |= FYEF_WHITESPACE;
	}
}

void fy_emit_node_comment(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent,
			  enum fy_comment_placement placement)
{
	struct fy_token *fyt;

	if (!fy_emit_output_comments(emit) || (unsigned int)placement >= fycp_max)
		return;

	fyt = fy_node_value_token(fyn);
	if (!fyt)
		return;

	fy_emit_token_comment(emit, fyt, flags, indent, placement);
}

void fy_emit_common_node_preamble(struct fy_emitter *emit,
		struct fy_token *fyt_anchor,
		struct fy_token *fyt_tag,
		int flags, int indent)
{
	const char *anchor = NULL;
	const char *tag = NULL;
	const char *td_prefix __FY_DEBUG_UNUSED__;
	const char *td_handle;
	size_t td_prefix_size, td_handle_size;
	size_t tag_len = 0, anchor_len = 0;
	bool json_mode = false;

	json_mode = fy_emit_is_json_mode(emit);

	if (!json_mode) {
		if (!(emit->cfg.flags & FYECF_STRIP_LABELS)) {
			if (fyt_anchor)
				anchor = fy_token_get_text(fyt_anchor, &anchor_len);
		}

		if (!(emit->cfg.flags & FYECF_STRIP_TAGS)) {
			if (fyt_tag)
				tag = fy_token_get_text(fyt_tag, &tag_len);
		}

		if (anchor) {
			fy_emit_write_indicator(emit, di_ambersand, flags, indent, fyewt_anchor);
			fy_emit_write(emit, fyewt_anchor, anchor, anchor_len);
		}

		if (tag) {
			if (!fy_emit_whitespace(emit))
				fy_emit_write_ws(emit);

			td_handle = fy_tag_token_get_directive_handle(fyt_tag, &td_handle_size);
			assert(td_handle);
			td_prefix = fy_tag_token_get_directive_prefix(fyt_tag, &td_prefix_size);
			assert(td_prefix);

			if (!td_handle_size)
				fy_emit_printf(emit, fyewt_tag, "!<%.*s>", (int)tag_len, tag);
			else
				fy_emit_printf(emit, fyewt_tag, "%.*s%.*s",
						(int)td_handle_size, td_handle,
						(int)(tag_len - td_prefix_size), tag + td_prefix_size);

			emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION);
		}
	}

	/* content for root always starts on a new line */
	if ((flags & DDNF_ROOT) && emit->column != 0 &&
            !(emit->flags & FYEF_HAD_DOCUMENT_START)) {
		fy_emit_putc(emit, fyewt_linebreak, '\n');
		emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
	}
}

void fy_emit_node_internal(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent, bool is_key)
{
	enum fy_node_type type;
	struct fy_anchor *fya;
	struct fy_token *fyt_anchor = NULL;

	if (!(emit->cfg.flags & FYECF_STRIP_LABELS)) {
		fya = fy_document_lookup_anchor_by_node(emit->fyd, fyn);
		if (fya)
			fyt_anchor = fya->anchor;
	}

	fy_emit_common_node_preamble(emit, fyt_anchor, fyn->tag, flags, indent);

	type = fyn ? fyn->type : FYNT_SCALAR;

	if (type != FYNT_SCALAR && (flags & DDNF_ROOT) && emit->column != 0) {
		fy_emit_putc(emit, fyewt_linebreak, '\n');
		emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
	}

	switch (type) {
	case FYNT_SCALAR:
		/* if we're pretty and root at column 0 (meaning it's a single scalar document) output --- */
		if ((flags & DDNF_ROOT) && fy_emit_is_pretty_mode(emit) && !emit->column &&
				!fy_emit_is_flow_mode(emit) && !(flags & DDNF_FLOW))
			fy_emit_document_start_indicator(emit);
		fy_emit_scalar(emit, fyn, flags, indent, is_key);
		break;
	case FYNT_SEQUENCE:
		FYD_TOKEN_ERROR_CHECK(fyn->fyd, fyn->sequence_start, FYEM_INTERNAL,
				!is_key || !fy_emit_is_json_mode(emit), err_out,
				"JSON does not allow sequences as keys");
		fy_emit_sequence(emit, fyn, flags, indent);
		break;
	case FYNT_MAPPING:
		FYD_TOKEN_ERROR_CHECK(fyn->fyd, fyn->mapping_start, FYEM_INTERNAL,
				!is_key || !fy_emit_is_json_mode(emit), err_out,
				"JSON does not allow mappings as keys");
		fy_emit_mapping(emit, fyn, flags, indent);
		break;
	}
err_out:
	/* nothing */
	return;
}

void fy_emit_token_write_plain(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent)
{
	bool allow_breaks, should_indent, spaces, breaks;
	int c;
	enum fy_emitter_write_type wtype;
	const char *str = NULL;
	size_t len = 0;
	struct fy_atom *atom;
	struct fy_atom_iter iter;

	/* null and not json mode */
	if (!fyt && !fy_emit_is_json_mode(emit))
		goto out;

	wtype = (flags & DDNF_SIMPLE_SCALAR_KEY) ? fyewt_plain_scalar_key : fyewt_plain_scalar;

	atom = fy_token_atom(fyt);

	/* null and json mode */
	if (fy_emit_is_json_mode(emit) && (/* (!fyt || !atom || atom->size0) || */ fyt->scalar.is_null)) {
		fy_emit_puts(emit, wtype, "null");
		goto out;
	}

	/* simple case first (90% of cases) */
	str = fy_token_get_direct_output(fyt, &len);
	if (str && fy_token_atom_style(fyt) == FYAS_PLAIN) {
		fy_emit_write(emit, wtype, str, len);
		goto out;
	}

	if (!atom)
		goto out;

	allow_breaks = !(flags & DDNF_SIMPLE) && !fy_emit_is_json_mode(emit) && !fy_emit_is_oneline(emit);

	spaces = false;
	breaks = false;

	fy_atom_iter_start(atom, &iter);
	fy_emit_accum_start(&emit->ea, emit->column, fy_token_atom_lb_mode(fyt));
	while ((c = fy_atom_iter_utf8_get(&iter)) > 0) {

		if (fy_is_ws(c)) {

			should_indent = allow_breaks && !spaces &&
					fy_emit_accum_column(&emit->ea) > fy_emit_width(emit);

			if (should_indent && !fy_is_ws(fy_atom_iter_utf8_peek(&iter))) {
				fy_emit_output_accum(emit, wtype, &emit->ea);
				emit->flags &= ~FYEF_INDENTATION;
				fy_emit_write_indent(emit, indent);
			} else
				fy_emit_accum_utf8_put(&emit->ea, c);

			spaces = true;

		} else if (fy_is_lb_m(c, fy_token_atom_lb_mode(fyt))) {

			/* blergh */
			if (!allow_breaks)
				break;

			/* output run */
			if (!breaks) {
				fy_emit_output_accum(emit, wtype, &emit->ea);
				fy_emit_write_indent(emit, indent);
			}

			emit->flags &= ~FYEF_INDENTATION;
			fy_emit_write_indent(emit, indent);

			breaks = true;

		} else {

			if (breaks)
				fy_emit_write_indent(emit, indent);

			fy_emit_accum_utf8_put(&emit->ea, c);

			emit->flags &= ~FYEF_INDENTATION;

			spaces = false;
			breaks = false;
		}
	}
	fy_emit_output_accum(emit, wtype, &emit->ea);
	fy_emit_accum_finish(&emit->ea);
	fy_atom_iter_finish(&iter);

out:
	emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION);
}

void fy_emit_token_write_alias(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent)
{
	const char *str = NULL;
	size_t len = 0;
	struct fy_atom_iter iter;
	int c;

	if (!fyt)
		return;

	fy_emit_write_indicator(emit, di_star, flags, indent, fyewt_alias);

	/* try direct output first (99% of cases) */
	str = fy_token_get_direct_output(fyt, &len);
	if (str) {
		fy_emit_write(emit, fyewt_alias, str, len);
		return;
	}

	/* corner case, use iterator */
	fy_atom_iter_start(fy_token_atom(fyt), &iter);
	fy_emit_accum_start(&emit->ea, emit->column, fy_token_atom_lb_mode(fyt));
	while ((c = fy_atom_iter_utf8_get(&iter)) > 0)
		fy_emit_accum_utf8_put(&emit->ea, c);
	fy_emit_output_accum(emit, fyewt_alias, &emit->ea);
	fy_emit_accum_finish(&emit->ea);
	fy_atom_iter_finish(&iter);
}

void fy_emit_token_write_quoted(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent, char qc)
{
	bool allow_breaks, spaces, breaks;
	int c, i, w, digit;
	enum fy_emitter_write_type wtype;
	const char *str = NULL;
	size_t len = 0;
	bool should_indent, done_esc;
	struct fy_atom *atom;
	struct fy_atom_iter iter;
	enum fy_atom_style target_style;
	uint32_t hi_surrogate, lo_surrogate;
	uint8_t non_utf8[4];
	size_t non_utf8_len, k;

	wtype = qc == '\'' ?
		((flags & DDNF_SIMPLE_SCALAR_KEY) ?
		 	fyewt_single_quoted_scalar_key : fyewt_single_quoted_scalar) :
		((flags & DDNF_SIMPLE_SCALAR_KEY) ?
		 	fyewt_double_quoted_scalar_key : fyewt_double_quoted_scalar);

	fy_emit_write_indicator(emit,
			qc == '\'' ? di_single_quote_start : di_double_quote_start,
			flags, indent, wtype);

	/* XXX check whether this is ever the case */
	if (!fyt)
		goto out;

	/* note that if the original target style and the target differ
	 * we can note use direct output
	 */
	target_style = qc == '"' ? FYAS_DOUBLE_QUOTED : FYAS_SINGLE_QUOTED;

	/* simple case of direct output (large amount of cases) */
	str = fy_token_get_direct_output(fyt, &len);
	if (str && fy_token_atom_style(fyt) == target_style) {
		fy_emit_write(emit, wtype, str, len);
		goto out;
	}

	/* no atom? i.e. empty */
	atom = fy_token_atom(fyt);
	if (!atom)
		goto out;

	allow_breaks = !(flags & DDNF_SIMPLE) && !fy_emit_is_json_mode(emit) && !fy_emit_is_oneline(emit);

	spaces = false;
	breaks = false;

	fy_atom_iter_start(atom, &iter);
	fy_emit_accum_start(&emit->ea, emit->column, fy_token_atom_lb_mode(fyt));
	for (;;) {
		non_utf8_len = sizeof(non_utf8);
		c = fy_atom_iter_utf8_quoted_get(&iter, &non_utf8_len, non_utf8);
		if (c < 0)
			break;

		if (c == 0 && non_utf8_len > 0) {
			for (k = 0; k < non_utf8_len; k++) {
				c = (int)non_utf8[k] & 0xff;
				fy_emit_accum_utf8_put(&emit->ea, '\\');
				fy_emit_accum_utf8_put(&emit->ea, 'x');
				digit = ((unsigned int)c >> 4) & 15;
				fy_emit_accum_utf8_put(&emit->ea,
						digit <= 9 ? ('0' + digit) : ('A' + digit - 10));
				digit = (unsigned int)c & 15;
				fy_emit_accum_utf8_put(&emit->ea,
						digit <= 9 ? ('0' + digit) : ('A' + digit - 10));
			}
			continue;
		}

		if (fy_is_space(c) || (qc == '\'' && fy_is_ws(c))) {
			should_indent = allow_breaks && !spaces &&
					fy_emit_accum_column(&emit->ea) > fy_emit_width(emit);

			if (should_indent &&
				((qc == '\'' && fy_is_ws(fy_atom_iter_utf8_peek(&iter))) ||
				  qc == '"')) {
				fy_emit_output_accum(emit, wtype, &emit->ea);

				if (qc == '"' && fy_is_ws(fy_atom_iter_utf8_peek(&iter)))
					fy_emit_putc(emit, wtype, '\\');

				emit->flags &= ~FYEF_INDENTATION;
				fy_emit_write_indent(emit, indent);
			} else
				fy_emit_accum_utf8_put(&emit->ea, c);

			spaces = true;
			breaks = false;

		} else if (qc == '\'' && fy_is_lb_m(c, fy_token_atom_lb_mode(fyt))) {

			/* blergh */
			if (!allow_breaks)
				break;

			/* output run */
			if (!breaks) {
				fy_emit_output_accum(emit, wtype, &emit->ea);
				fy_emit_write_indent(emit, indent);
			}

			emit->flags &= ~FYEF_INDENTATION;
			fy_emit_write_indent(emit, indent);

			breaks = true;
		} else {
			/* output run */
			if (breaks) {
				fy_emit_output_accum(emit, wtype, &emit->ea);
				fy_emit_write_indent(emit, indent);
			}

			/* escape */
			if (qc == '\'' && c == '\'') {
				fy_emit_accum_utf8_put(&emit->ea, '\'');
				fy_emit_accum_utf8_put(&emit->ea, '\'');
			} else if (qc == '"' &&
				   ((!fy_is_printq(c) || c == '"' || c == '\\') ||
				    (fy_emit_is_json_mode(emit) && !fy_is_json_unescaped(c)))) {

				fy_emit_accum_utf8_put(&emit->ea, '\\');

				/* common YAML and JSON escapes */
				done_esc = false;
				switch (c) {
				case '\b':
					fy_emit_accum_utf8_put(&emit->ea, 'b');
					done_esc = true;
					break;
				case '\f':
					fy_emit_accum_utf8_put(&emit->ea, 'f');
					done_esc = true;
					break;
				case '\n':
					fy_emit_accum_utf8_put(&emit->ea, 'n');
					done_esc = true;
					break;
				case '\r':
					fy_emit_accum_utf8_put(&emit->ea, 'r');
					done_esc = true;
					break;
				case '\t':
					fy_emit_accum_utf8_put(&emit->ea, 't');
					done_esc = true;
					break;
				case '"':
					fy_emit_accum_utf8_put(&emit->ea, '"');
					done_esc = true;
					break;
				case '\\':
					fy_emit_accum_utf8_put(&emit->ea, '\\');
					done_esc = true;
					break;
				}

				if (done_esc)
					goto done;

				if (!fy_emit_is_json_mode(emit)) {
					switch (c) {
					case '\0':
						fy_emit_accum_utf8_put(&emit->ea, '0');
						break;
					case '\a':
						fy_emit_accum_utf8_put(&emit->ea, 'a');
						break;
					case '\v':
						fy_emit_accum_utf8_put(&emit->ea, 'v');
						break;
					case '\e':
						fy_emit_accum_utf8_put(&emit->ea, 'e');
						break;
					case 0x85:
						fy_emit_accum_utf8_put(&emit->ea, 'N');
						break;
					case 0xa0:
						fy_emit_accum_utf8_put(&emit->ea, '_');
						break;
					case 0x2028:
						fy_emit_accum_utf8_put(&emit->ea, 'L');
						break;
					case 0x2029:
						fy_emit_accum_utf8_put(&emit->ea, 'P');
						break;
					default:
						if ((unsigned int)c <= 0xff) {
							fy_emit_accum_utf8_put(&emit->ea, 'x');
							w = 2;
						} else if ((unsigned int)c <= 0xffff) {
							fy_emit_accum_utf8_put(&emit->ea, 'u');
							w = 4;
						} else if ((unsigned int)c <= 0xffffffff) {
							fy_emit_accum_utf8_put(&emit->ea, 'U');
							w = 8;
						}

						for (i = w - 1; i >= 0; i--) {
							digit = ((unsigned int)c >> (i * 4)) & 15;
							fy_emit_accum_utf8_put(&emit->ea,
									digit <= 9 ? ('0' + digit) : ('A' + digit - 10));
						}
						break;
					}

				} else {
					/* JSON escapes all others in \uXXXX and \uXXXX\uXXXX */
					w = 4;
					if ((unsigned int)c <= 0xffff) {
						fy_emit_accum_utf8_put(&emit->ea, 'u');
						for (i = w - 1; i >= 0; i--) {
							digit = ((unsigned int)c >> (i * 4)) & 15;
							fy_emit_accum_utf8_put(&emit->ea,
									digit <= 9 ? ('0' + digit) : ('A' + digit - 10));
						}
					} else {
						hi_surrogate = 0xd800 | ((((c >> 16) & 0x1f) - 1) << 6) | ((c >> 10) & 0x3f);
						lo_surrogate = 0xdc00 | (c & 0x3ff);

						fy_emit_accum_utf8_put(&emit->ea, 'u');
						for (i = w - 1; i >= 0; i--) {
							digit = ((unsigned int)hi_surrogate >> (i * 4)) & 15;
							fy_emit_accum_utf8_put(&emit->ea,
									digit <= 9 ? ('0' + digit) : ('A' + digit - 10));
						}

						fy_emit_accum_utf8_put(&emit->ea, '\\');
						fy_emit_accum_utf8_put(&emit->ea, 'u');
						for (i = w - 1; i >= 0; i--) {
							digit = ((unsigned int)lo_surrogate >> (i * 4)) & 15;
							fy_emit_accum_utf8_put(&emit->ea,
									digit <= 9 ? ('0' + digit) : ('A' + digit - 10));
						}
					}
				}
			} else
				fy_emit_accum_utf8_put(&emit->ea, c);
done:
			emit->flags &= ~FYEF_INDENTATION;
			spaces = false;
			breaks = false;
		}
	}
	fy_emit_output_accum(emit, wtype, &emit->ea);
	fy_emit_accum_finish(&emit->ea);
	fy_atom_iter_finish(&iter);

out:
	fy_emit_write_indicator(emit,
			qc == '\'' ? di_single_quote_end : di_double_quote_end,
			flags, indent, wtype);
}

bool fy_emit_token_write_block_hints(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent, char *chompp)
{
	char chomp = '\0';
	bool explicit_chomp = false;
	struct fy_atom *atom;

	atom = fy_token_atom(fyt);
	if (!atom) {
		emit->flags &= ~FYEF_OPEN_ENDED;
		chomp = '-';
		goto out;
	}

	if (atom->starts_with_ws || atom->starts_with_lb) {
		fy_emit_putc(emit, fyewt_indicator, '0' + fy_emit_indent(emit));
		explicit_chomp = true;
	}

	if (!atom->ends_with_lb) {
		emit->flags &= ~FYEF_OPEN_ENDED;
		chomp = '-';
		goto out;
	}

	if (atom->trailing_lb) {
		emit->flags |= FYEF_OPEN_ENDED;
		chomp = '+';
		goto out;
	}
	emit->flags &= ~FYEF_OPEN_ENDED;

out:
	if (chomp)
		fy_emit_putc(emit, fyewt_indicator, chomp);
	*chompp = chomp;
	return explicit_chomp;
}

void fy_emit_token_write_literal(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent)
{
	bool breaks;
	int c;
	char chomp;
	struct fy_atom *atom;
	struct fy_atom_iter iter;

	fy_emit_write_indicator(emit, di_bar, flags, indent, fyewt_indicator);

	fy_emit_token_write_block_hints(emit, fyt, flags, indent, &chomp);
	if (flags & DDNF_ROOT)
		indent += fy_emit_indent(emit);

	fy_emit_putc(emit, fyewt_linebreak, '\n');
	emit->flags |= FYEF_WHITESPACE | FYEF_INDENTATION;

	atom = fy_token_atom(fyt);
	if (!atom)
		goto out;

	breaks = true;

	fy_atom_iter_start(atom, &iter);
	fy_emit_accum_start(&emit->ea, emit->column, fy_token_atom_lb_mode(fyt));
	while ((c = fy_atom_iter_utf8_get(&iter)) > 0) {

		if (breaks) {
			fy_emit_write_indent(emit, indent);
			breaks = false;
		}

		if (fy_is_lb_m(c, fy_token_atom_lb_mode(fyt))) {
			fy_emit_output_accum(emit, fyewt_literal_scalar, &emit->ea);
			emit->flags &= ~FYEF_INDENTATION;
			breaks = true;
		} else
			fy_emit_accum_utf8_put(&emit->ea, c);
	}
	fy_emit_output_accum(emit, fyewt_literal_scalar, &emit->ea);
	fy_emit_accum_finish(&emit->ea);
	fy_atom_iter_finish(&iter);

out:
	emit->flags &= ~FYEF_INDENTATION;
}

void fy_emit_token_write_folded(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent)
{
	bool leading_spaces, breaks;
	int c, nrbreaks, nrbreakslim;
	char chomp;
	struct fy_atom *atom;
	struct fy_atom_iter iter;

	fy_emit_write_indicator(emit, di_greater, flags, indent, fyewt_indicator);

	fy_emit_token_write_block_hints(emit, fyt, flags, indent, &chomp);
	if (flags & DDNF_ROOT)
		indent += fy_emit_indent(emit);

	fy_emit_putc(emit, fyewt_linebreak, '\n');
	emit->flags |= FYEF_WHITESPACE | FYEF_INDENTATION;

	atom = fy_token_atom(fyt);
	if (!atom)
		return;

	breaks = true;
	leading_spaces = true;

	fy_atom_iter_start(atom, &iter);
	fy_emit_accum_start(&emit->ea, emit->column, fy_token_atom_lb_mode(fyt));
	while ((c = fy_atom_iter_utf8_get(&iter)) > 0) {

		if (fy_is_lb_m(c, fy_token_atom_lb_mode(fyt))) {

			/* output run */
			if (!fy_emit_accum_empty(&emit->ea)) {
				fy_emit_output_accum(emit, fyewt_literal_scalar, &emit->ea);
				/* do not output a newline (indent) if at the end or
				 * this is a leading spaces line */
				if (!fy_is_z(fy_atom_iter_utf8_peek(&iter)) && !leading_spaces)
					fy_emit_write_indent(emit, indent);
			}

			/* count the number of consecutive breaks */
			nrbreaks = 1;
			while (fy_is_lb_m((c = fy_atom_iter_utf8_peek(&iter)), fy_token_atom_lb_mode(fyt))) {
				nrbreaks++;
				(void)fy_atom_iter_utf8_get(&iter);
			}

			/* NOTE: Because the number of indents is tricky
			 * if it's a non blank, non end, it's the number of breaks
			 * if it's a blank, it's the number of breaks minus 1
			 * if it's the end, it's the number of breaks minus 2
			 */
			nrbreakslim = fy_is_z(c) ? 2 : fy_is_blank(c) ? 1 : 0;
			while (nrbreaks-- > nrbreakslim) {
				emit->flags &= ~FYEF_INDENTATION;
				fy_emit_write_indent(emit, indent);
			}

			breaks = true;

		} else {

			/* if we had a break, output an indent */
			if (breaks) {
				fy_emit_write_indent(emit, indent);

				/* if this line starts with whitespace we need to know */
				leading_spaces = fy_is_ws(c);
			}

			if (!breaks && fy_is_space(c) &&
			    !fy_is_space(fy_atom_iter_utf8_peek(&iter)) &&
			    fy_emit_accum_column(&emit->ea) > fy_emit_width(emit)) {
				fy_emit_output_accum(emit, fyewt_folded_scalar, &emit->ea);
				emit->flags &= ~FYEF_INDENTATION;
				fy_emit_write_indent(emit, indent);
			} else
				fy_emit_accum_utf8_put(&emit->ea, c);

			breaks = false;
		}
	}
	fy_emit_output_accum(emit, fyewt_folded_scalar, &emit->ea);
	fy_emit_accum_finish(&emit->ea);
	fy_atom_iter_finish(&iter);
}

static enum fy_node_style
fy_emit_token_scalar_style(struct fy_emitter *emit, struct fy_token *fyt,
			   int flags, int indent, enum fy_node_style style,
			   struct fy_token *fyt_tag)
{
	const char *value = NULL;
	size_t len = 0;
	bool json, flow, is_null_scalar, is_json_plain;
	struct fy_atom *atom;
	int aflags = -1;
	const char *tag;
	size_t tag_len;

	atom = fy_token_atom(fyt);

	flow = fy_emit_is_flow_mode(emit) || (flags & DDNF_FLOW);

	/* check if style is allowed (i.e. no block styles in flow context) */
	if (flow && (style == FYNS_LITERAL || style == FYNS_FOLDED))
		style = FYNS_ANY;

	json = fy_emit_is_json_mode(emit);

	is_null_scalar = !atom || fyt->scalar.is_null;

	/* is this a plain json atom? */
	is_json_plain = (json || emit->source_json || fy_emit_is_dejson_mode(emit)) &&
			(is_null_scalar ||
			!fy_atom_strcmp(atom, "false") ||
			!fy_atom_strcmp(atom, "true") ||
			!fy_atom_strcmp(atom, "null") ||
			fy_atom_is_number(atom));

	if (json) {
		/* literal in JSON mode is output as quoted */
		if (style == FYNS_LITERAL || style == FYNS_FOLDED)
			return FYNS_DOUBLE_QUOTED;

		if (is_json_plain) {
			tag = fy_token_get_text(fyt_tag, &tag_len);

			/* XXX hardcoded string tag resultion */
			if (tag && tag_len &&
			     ((tag_len == 1 && *tag == '!') ||
			      (tag_len == 21 && !memcmp(tag, "tag:yaml.org,2002:str", 21))))
				return FYNS_DOUBLE_QUOTED;
		}

		/* JSON NULL, but with plain style */
		if ((style == FYNS_PLAIN || style == FYNS_ANY) &&
		    (is_null_scalar || (is_json_plain && !atom->size0)))
			return FYNS_PLAIN;

		return FYNS_DOUBLE_QUOTED;
	}

	aflags = fy_token_text_analyze(fyt);

	if (flow && (style == FYNS_ANY || style == FYNS_LITERAL || style == FYNS_FOLDED)) {

		if (fyt && !value)
			value = fy_token_get_text(fyt, &len);

		/* if there's a linebreak, use double quoted style */
		if (fy_find_any_lb(value, len)) {
			style = FYNS_DOUBLE_QUOTED;
			goto out;
		}

		/* check if there's a non printable */
		if (!fy_find_non_print(value, len)) {
			style = FYNS_SINGLE_QUOTED;
			goto out;
		}

		/* anything not empty is double quoted here */
		style = !(aflags & FYTTAF_EMPTY) ? FYNS_PLAIN : FYNS_DOUBLE_QUOTED;
	}

	/* try to pretify */
	if (!flow && fy_emit_is_pretty_mode(emit) &&
		(style == FYNS_ANY || style == FYNS_DOUBLE_QUOTED || style == FYNS_SINGLE_QUOTED)) {

		/* any original style can be a plain, but contains linebreaks, do a literal */
		if ((aflags & (FYTTAF_CAN_BE_PLAIN | FYTTAF_HAS_LB)) == (FYTTAF_CAN_BE_PLAIN | FYTTAF_HAS_LB)) {
			style = FYNS_LITERAL;
			goto out;
		}

		/* any style, can be just a plain, just make it so */
		if (style == FYNS_ANY && (aflags & (FYTTAF_CAN_BE_PLAIN | FYTTAF_HAS_LB)) == FYTTAF_CAN_BE_PLAIN) {
			style = FYNS_PLAIN;
			goto out;
		}

	}

	if (!flow && emit->source_json && fy_emit_is_dejson_mode(emit)) {
		if (is_json_plain || (aflags & (FYTTAF_CAN_BE_PLAIN | FYTTAF_HAS_LB)) == FYTTAF_CAN_BE_PLAIN) {
			style = FYNS_PLAIN;
			goto out;
		}
	}

out:
	if (style == FYNS_ANY) {
		if (fyt)
			value = fy_token_get_text(fyt, &len);

		style = (aflags & FYTTAF_CAN_BE_PLAIN) ?
				FYNS_PLAIN : FYNS_DOUBLE_QUOTED;
	}

	if (style == FYNS_PLAIN) {
		/* plains in flow mode not being able to be plains
		 * - plain in block mode that can't be plain in flow mode
		 * - special handling for plains on start of line
		 */
		if ((flow && !(aflags & FYTTAF_CAN_BE_PLAIN_FLOW) && !is_null_scalar) ||
		    ((aflags & FYTTAF_QUOTE_AT_0) && indent == 0))
			style = FYNS_DOUBLE_QUOTED;
	}

	return style;
}

void fy_emit_token_scalar(struct fy_emitter *emit, struct fy_token *fyt, int flags, int indent,
			  enum fy_node_style style, struct fy_token *fyt_tag)
{
	assert(style != FYNS_FLOW && style != FYNS_BLOCK);

	indent = fy_emit_increase_indent(emit, flags, indent);

	if (!fy_emit_whitespace(emit))
		fy_emit_write_ws(emit);

	style = fy_emit_token_scalar_style(emit, fyt, flags, indent, style, fyt_tag);

	switch (style) {
	case FYNS_ALIAS:
		fy_emit_token_write_alias(emit, fyt, flags, indent);
		break;
	case FYNS_PLAIN:
		fy_emit_token_write_plain(emit, fyt, flags, indent);
		break;
	case FYNS_DOUBLE_QUOTED:
		fy_emit_token_write_quoted(emit, fyt, flags, indent, '"');
		break;
	case FYNS_SINGLE_QUOTED:
		fy_emit_token_write_quoted(emit, fyt, flags, indent, '\'');
		break;
	case FYNS_LITERAL:
		fy_emit_token_write_literal(emit, fyt, flags, indent);
		break;
	case FYNS_FOLDED:
		fy_emit_token_write_folded(emit, fyt, flags, indent);
		break;
	default:
		break;
	}
}

void fy_emit_scalar(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent, bool is_key)
{
	enum fy_node_style style;

	/* default style */
	style = fyn ? fyn->style : FYNS_ANY;

	/* all JSON keys are double quoted */
	if (fy_emit_is_json_mode(emit) && is_key)
		style = FYNS_DOUBLE_QUOTED;

	fy_emit_token_scalar(emit,
			fyn ? fyn->scalar : NULL,
			flags, indent,
			style, fyn->tag);
}

static void fy_emit_sequence_prolog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc)
{
	bool json = fy_emit_is_json_mode(emit);
	bool oneline = fy_emit_is_oneline(emit);
	bool was_flow = sc->flow;

	sc->old_indent = sc->indent;
	if (!json) {
		if (fy_emit_is_manual(emit))
			sc->flow = (sc->xstyle == FYNS_BLOCK && was_flow) || sc->xstyle == FYNS_FLOW;
		else if (fy_emit_is_block_mode(emit))
			sc->flow = sc->empty;
		else
			sc->flow = fy_emit_is_flow_mode(emit) || emit->flow_level || sc->flow_token || sc->empty;

		if (sc->flow) {
			if (!emit->flow_level) {
				sc->indent = fy_emit_increase_indent(emit, sc->flags, sc->indent);
				sc->old_indent = sc->indent;
			}

			sc->flags = (sc->flags | DDNF_FLOW) | (sc->flags & ~DDNF_INDENTLESS);
			fy_emit_write_indicator(emit, di_left_bracket, sc->flags, sc->indent, fyewt_indicator);
		} else {
			sc->flags = (sc->flags & ~DDNF_FLOW);
		}
	} else {
		sc->flags = (sc->flags | DDNF_FLOW) | (sc->flags & ~DDNF_INDENTLESS);
		fy_emit_write_indicator(emit, di_left_bracket, sc->flags, sc->indent, fyewt_indicator);
	}

	if (!oneline) {
		if (was_flow || (sc->flags & (DDNF_ROOT | DDNF_SEQ)))
			sc->indent = fy_emit_increase_indent(emit, sc->flags, sc->indent);
	}

	sc->flags &= ~DDNF_ROOT;
}

static void fy_emit_sequence_epilog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc)
{
	if (sc->flow || fy_emit_is_json_mode(emit)) {
		if (!fy_emit_is_oneline(emit) && !sc->empty)
			fy_emit_write_indent(emit, sc->old_indent);
		fy_emit_write_indicator(emit, di_right_bracket, sc->flags, sc->old_indent, fyewt_indicator);
	}
}

static void fy_emit_sequence_item_prolog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc,
					 struct fy_token *fyt_value)
{
	int tmp_indent;

	sc->flags |= DDNF_SEQ;

	if (!fy_emit_is_oneline(emit))
		fy_emit_write_indent(emit, sc->indent);

	if (!sc->flow && !fy_emit_is_json_mode(emit))
		fy_emit_write_indicator(emit, di_dash, sc->flags, sc->indent, fyewt_indicator);

	tmp_indent = sc->indent;
	if (fy_emit_token_has_comment(emit, fyt_value, fycp_top)) {
		if (!sc->flow && !fy_emit_is_json_mode(emit))
			tmp_indent = fy_emit_increase_indent(emit, sc->flags, sc->indent);
		fy_emit_token_comment(emit, fyt_value, sc->flags, tmp_indent, fycp_top);
	}
}

static void fy_emit_sequence_item_epilog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc,
					 bool last, struct fy_token *fyt_value)
{
	if ((sc->flow || fy_emit_is_json_mode(emit)) && !last)
		fy_emit_write_indicator(emit, di_comma, sc->flags, sc->indent, fyewt_indicator);

	fy_emit_token_comment(emit, fyt_value, sc->flags, sc->indent, fycp_right);

	if (last && (sc->flow || fy_emit_is_json_mode(emit)) && !fy_emit_is_oneline(emit) && !sc->empty)
		fy_emit_write_indent(emit, sc->old_indent);

	sc->flags &= ~DDNF_SEQ;
}

void fy_emit_sequence(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent)
{
	struct fy_node *fyni, *fynin;
	struct fy_token *fyt_value;
	bool last;
	struct fy_emit_save_ctx sct, *sc = &sct;

	memset(sc, 0, sizeof(*sc));

	sc->flags = flags;
	sc->indent = indent;
	sc->empty = fy_node_list_empty(&fyn->sequence);
	sc->flow_token = fyn->style == FYNS_FLOW;
	sc->flow = !!(flags & DDNF_FLOW);
	sc->xstyle = fyn->style;
	sc->old_indent = sc->indent;

	fy_emit_sequence_prolog(emit, sc);

	for (fyni = fy_node_list_head(&fyn->sequence); fyni; fyni = fynin) {

		fynin = fy_node_next(&fyn->sequence, fyni);
		last = !fynin;
		fyt_value = fy_node_value_token(fyni);

		fy_emit_sequence_item_prolog(emit, sc, fyt_value);
		fy_emit_node_internal(emit, fyni, (sc->flags & ~DDNF_ROOT), sc->indent, false);
		fy_emit_sequence_item_epilog(emit, sc, last, fyt_value);
	}

	fy_emit_sequence_epilog(emit, sc);
}

static void fy_emit_mapping_prolog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc)
{
	bool json = fy_emit_is_json_mode(emit);
	bool oneline = fy_emit_is_oneline(emit);
	bool was_flow = sc->flow;

	sc->old_indent = sc->indent;
	if (!json) {
		if (fy_emit_is_manual(emit))
			sc->flow = (sc->xstyle == FYNS_BLOCK && was_flow) || sc->xstyle == FYNS_FLOW;
		else if (fy_emit_is_block_mode(emit))
			sc->flow = sc->empty;
		else
			sc->flow = fy_emit_is_flow_mode(emit) || emit->flow_level || sc->flow_token || sc->empty;

		if (sc->flow) {
			if (!emit->flow_level) {
				sc->indent = fy_emit_increase_indent(emit, sc->flags, sc->indent);
				sc->old_indent = sc->indent;
			}

			sc->flags = (sc->flags | DDNF_FLOW) | (sc->flags & ~DDNF_INDENTLESS);
			fy_emit_write_indicator(emit, di_left_brace, sc->flags, sc->indent, fyewt_indicator);
		} else {
			sc->flags &= ~(DDNF_FLOW | DDNF_INDENTLESS);
		}
	} else {
		sc->flags = (sc->flags | DDNF_FLOW) | (sc->flags & ~DDNF_INDENTLESS);
		fy_emit_write_indicator(emit, di_left_brace, sc->flags, sc->indent, fyewt_indicator);
	}

	if (!oneline && !sc->empty)
		sc->indent = fy_emit_increase_indent(emit, sc->flags, sc->indent);

	sc->flags &= ~DDNF_ROOT;
}

static void fy_emit_mapping_epilog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc)
{
	if (sc->flow || fy_emit_is_json_mode(emit)) {
		if (!fy_emit_is_oneline(emit) && !sc->empty)
			fy_emit_write_indent(emit, sc->old_indent);
		fy_emit_write_indicator(emit, di_right_brace, sc->flags, sc->old_indent, fyewt_indicator);
	}
}

static void fy_emit_mapping_key_prolog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc,
				       struct fy_token *fyt_key, bool simple_key)
{
	sc->flags = DDNF_MAP | (sc->flags & DDNF_FLOW);

	if (simple_key) {
		sc->flags |= DDNF_SIMPLE;
		if (fyt_key && fyt_key->type == FYTT_SCALAR)
			sc->flags |= DDNF_SIMPLE_SCALAR_KEY;
	} else {
		/* do not emit the ? in flow modes at all */
		if (fy_emit_is_flow_mode(emit))
			sc->flags |= DDNF_SIMPLE;
	}

	if (!fy_emit_is_oneline(emit))
		fy_emit_write_indent(emit, sc->indent);

	/* complex? */
	if (!(sc->flags & DDNF_SIMPLE))
		fy_emit_write_indicator(emit, di_question_mark, sc->flags, sc->indent, fyewt_indicator);
}

static void fy_emit_mapping_key_epilog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc,
				       struct fy_token *fyt_key)
{
	int tmp_indent;

	/* if the key is an alias, always output an extra whitespace */
	if (fyt_key && fyt_key->type == FYTT_ALIAS)
		fy_emit_write_ws(emit);

	sc->flags &= ~DDNF_MAP;

	fy_emit_write_indicator(emit, di_colon, sc->flags, sc->indent, fyewt_indicator);

	tmp_indent = sc->indent;
	if (fy_emit_token_has_comment(emit, fyt_key, fycp_right)) {

		if (!sc->flow && !fy_emit_is_json_mode(emit))
			tmp_indent = fy_emit_increase_indent(emit, sc->flags, sc->indent);

		fy_emit_token_comment(emit, fyt_key, sc->flags, tmp_indent, fycp_right);
		fy_emit_write_indent(emit, tmp_indent);
	}

	sc->flags = DDNF_MAP | (sc->flags & DDNF_FLOW);
}

static void fy_emit_mapping_value_prolog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc,
					 struct fy_token *fyt_value)
{
	/* nothing */
}

static void fy_emit_mapping_value_epilog(struct fy_emitter *emit, struct fy_emit_save_ctx *sc,
					 bool last, struct fy_token *fyt_value)
{
	if ((sc->flow || fy_emit_is_json_mode(emit)) && !last)
		fy_emit_write_indicator(emit, di_comma, sc->flags, sc->indent, fyewt_indicator);

	fy_emit_token_comment(emit, fyt_value, sc->flags, sc->indent, fycp_right);

	if (last && (sc->flow || fy_emit_is_json_mode(emit)) && !fy_emit_is_oneline(emit) && !sc->empty)
		fy_emit_write_indent(emit, sc->old_indent);

	sc->flags &= ~DDNF_MAP;
}

void fy_emit_mapping(struct fy_emitter *emit, struct fy_node *fyn, int flags, int indent)
{
	struct fy_node_pair *fynp, *fynpn, **fynpp = NULL;
	struct fy_token *fyt_key, *fyt_value;
	bool last, simple_key, used_malloc = false;
	int aflags, i, count;
	struct fy_emit_save_ctx sct, *sc = &sct;

	memset(sc, 0, sizeof(*sc));

	sc->flags = flags;
	sc->indent = indent;
	sc->empty = fy_node_pair_list_empty(&fyn->mapping);
	sc->flow_token = fyn->style == FYNS_FLOW;
	sc->flow = !!(flags & DDNF_FLOW);
	sc->xstyle = fyn->style;
	sc->old_indent = sc->indent;

	fy_emit_mapping_prolog(emit, sc);

	if (!(emit->cfg.flags & (FYECF_SORT_KEYS | FYECF_STRIP_EMPTY_KV))) {
		fynp = fy_node_pair_list_head(&fyn->mapping);
		fynpp = NULL;
	} else {
		count = fy_node_mapping_item_count(fyn);

		/* heuristic, avoid allocation for small maps */
		if (count > 64) {
			fynpp = malloc((count + 1) * sizeof(*fynpp));
			fyd_error_check(fyn->fyd, fynpp, err_out,
					"malloc() failed");
			used_malloc = true;
		} else
			fynpp = FY_ALLOCA((count + 1) * sizeof(*fynpp));

		/* fill (removing empty KVs) */
		i = 0;
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp;
				fynp = fy_node_pair_next(&fyn->mapping, fynp)) {

			/* strip key/value pair from the output if it's empty */
			if ((emit->cfg.flags & FYECF_STRIP_EMPTY_KV) && fy_node_is_empty(fynp->value))
				continue;

			fynpp[i++] = fynp;
		}
		count = i;
		fynpp[count] = NULL;

		/* sort the keys */
		if (emit->cfg.flags & FYECF_SORT_KEYS)
			fy_node_mapping_perform_sort(fyn, NULL, NULL, fynpp, count);

		i = 0;
		fynp = fynpp[i];
	}

	for (; fynp; fynp = fynpn) {

		if (!fynpp)
			fynpn = fy_node_pair_next(&fyn->mapping, fynp);
		else
			fynpn = fynpp[++i];

		last = !fynpn;
		fyt_key = fy_node_value_token(fynp->key);
		fyt_value = fy_node_value_token(fynp->value);

		FYD_NODE_ERROR_CHECK(fynp->fyd, fynp->key, FYEM_INTERNAL,
				!fy_emit_is_json_mode(emit) ||
					(fynp->key && fynp->key->type == FYNT_SCALAR),
					err_out, "Non scalar keys are not allowed in JSON emit mode");

		simple_key = false;
		if (fynp->key) {
			switch (fynp->key->type) {
			case FYNT_SCALAR:
				aflags = fy_token_text_analyze(fynp->key->scalar);
				simple_key = fy_emit_is_json_mode(emit) ||
					     !!(aflags & FYTTAF_CAN_BE_SIMPLE_KEY);
				break;
			case FYNT_SEQUENCE:
				simple_key = fy_node_list_empty(&fynp->key->sequence);
				break;
			case FYNT_MAPPING:
				simple_key = fy_node_pair_list_empty(&fynp->key->mapping);
				break;
			}
		}

		fy_emit_mapping_key_prolog(emit, sc, fyt_key, simple_key);
		if (fynp->key)
			fy_emit_node_internal(emit, fynp->key, (sc->flags & ~DDNF_ROOT), sc->indent, true);
		fy_emit_mapping_key_epilog(emit, sc, fyt_key);

		fy_emit_mapping_value_prolog(emit, sc, fyt_value);
		if (fynp->value)
			fy_emit_node_internal(emit, fynp->value, (sc->flags & ~DDNF_ROOT), sc->indent, false);
		fy_emit_mapping_value_epilog(emit, sc, last, fyt_value);
	}

	if (fynpp && used_malloc)
		free(fynpp);

	fy_emit_mapping_epilog(emit, sc);

err_out:
	return;
}

int fy_emit_common_document_start(struct fy_emitter *emit,
				  struct fy_document_state *fyds,
				  bool root_tag_or_anchor)
{
	struct fy_token *fyt_chk;
	const char *td_handle, *td_prefix;
	size_t td_handle_size, td_prefix_size;
	enum fy_emitter_cfg_flags flags = emit->cfg.flags;
	enum fy_emitter_cfg_flags vd_flags = flags & FYECF_VERSION_DIR(FYECF_VERSION_DIR_MASK);
	enum fy_emitter_cfg_flags td_flags = flags & FYECF_TAG_DIR(FYECF_TAG_DIR_MASK);
	enum fy_emitter_cfg_flags dsm_flags = flags & FYECF_DOC_START_MARK(FYECF_DOC_START_MARK_MASK);
	bool vd, td, dsm;
	bool had_non_default_tag = false;

	if (!emit || !fyds || emit->fyds)
		return -1;

	emit->fyds = fyds;

	vd = ((vd_flags == FYECF_VERSION_DIR_AUTO && fyds->version_explicit) ||
	       vd_flags == FYECF_VERSION_DIR_ON) &&
	      !(emit->cfg.flags & FYECF_STRIP_DOC);
	td = ((td_flags == FYECF_TAG_DIR_AUTO && fyds->tags_explicit) ||
	       td_flags == FYECF_TAG_DIR_ON) &&
	      !(emit->cfg.flags & FYECF_STRIP_DOC);

	/* if either a version or directive tags exist, and no previous
	 * explicit document end existed, output one now
	 */
	if (!fy_emit_is_json_mode(emit) && (vd || td) && !(emit->flags & FYEF_HAD_DOCUMENT_END)) {
		if (emit->column)
			fy_emit_putc(emit, fyewt_linebreak, '\n');
		if (!(emit->cfg.flags & FYECF_STRIP_DOC)) {
			fy_emit_puts(emit, fyewt_document_indicator, "...");
			emit->flags &= ~FYEF_WHITESPACE;
			emit->flags |= FYEF_HAD_DOCUMENT_END;
		}
	}

	if (!fy_emit_is_json_mode(emit) && vd) {
		if (emit->column)
			fy_emit_putc(emit, fyewt_linebreak, '\n');
		fy_emit_printf(emit, fyewt_version_directive, "%%YAML %d.%d",
					fyds->version.major, fyds->version.minor);
		fy_emit_putc(emit, fyewt_linebreak, '\n');
		emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
	}

	if (!fy_emit_is_json_mode(emit) && td) {

		for (fyt_chk = fy_token_list_first(&fyds->fyt_td); fyt_chk; fyt_chk = fy_token_next(&fyds->fyt_td, fyt_chk)) {

			td_handle = fy_tag_directive_token_handle(fyt_chk, &td_handle_size);
			td_prefix = fy_tag_directive_token_prefix(fyt_chk, &td_prefix_size);
			assert(td_handle && td_prefix);

			if (fy_tag_is_default_internal(td_handle, td_handle_size, td_prefix, td_prefix_size))
				continue;

			had_non_default_tag = true;

			if (emit->column)
				fy_emit_putc(emit, fyewt_linebreak, '\n');
			fy_emit_printf(emit, fyewt_tag_directive, "%%TAG %.*s %.*s",
					(int)td_handle_size, td_handle,
					(int)td_prefix_size, td_prefix);
			fy_emit_putc(emit, fyewt_linebreak, '\n');
			emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
		}
	}

	/* always output document start indicator:
	 * - was explicit
	 * - document has tags
	 * - document has an explicit version
	 * - root exists & has a tag or an anchor
	 */
	dsm = (dsm_flags == FYECF_DOC_START_MARK_AUTO &&
			(!fyds->start_implicit ||
			  fyds->tags_explicit || fyds->version_explicit ||
			  had_non_default_tag)) ||
	       dsm_flags == FYECF_DOC_START_MARK_ON;

	/* if there was previous output without document end */
	if (!dsm && (emit->flags & FYEF_HAD_DOCUMENT_OUTPUT) &&
	           !(emit->flags & FYEF_HAD_DOCUMENT_END))
		dsm = true;

	/* output document start indicator if we should */
	if (dsm)
		fy_emit_document_start_indicator(emit);

	/* clear that in any case */
	emit->flags &= ~FYEF_HAD_DOCUMENT_END;

	return 0;
}

int fy_emit_document_start(struct fy_emitter *emit, struct fy_document *fyd,
			   struct fy_node *fyn_root)
{
	struct fy_node *root;
	bool root_tag_or_anchor;
	int ret;

	if (!emit || !fyd || !fyd->fyds)
		return -1;

	root = fyn_root ? fyn_root : fy_document_root(fyd);

	root_tag_or_anchor = root && (root->tag || fy_document_lookup_anchor_by_node(fyd, root));

	ret = fy_emit_common_document_start(emit, fyd->fyds, root_tag_or_anchor);
	if (ret)
		return ret;

	emit->fyd = fyd;

	return 0;
}

int fy_emit_common_document_end(struct fy_emitter *emit, bool override_state, bool implicit_override)
{
	const struct fy_document_state *fyds;
	enum fy_emitter_cfg_flags flags = emit->cfg.flags;
	enum fy_emitter_cfg_flags dem_flags = flags & FYECF_DOC_END_MARK(FYECF_DOC_END_MARK_MASK);
	bool implicit, dem;

	if (!emit || !emit->fyds)
		return -1;

	fyds = emit->fyds;

	implicit = fyds->end_implicit;
	if (override_state)
		implicit = implicit_override;

	dem = ((dem_flags == FYECF_DOC_END_MARK_AUTO && !implicit) ||
		dem_flags == FYECF_DOC_END_MARK_ON) &&
	       !(emit->cfg.flags & FYECF_STRIP_DOC);

	if (!(emit->cfg.flags & FYECF_NO_ENDING_NEWLINE)) {
		if (emit->column != 0) {
			fy_emit_putc(emit, fyewt_linebreak, '\n');
			emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
		}

		if (!fy_emit_is_json_mode(emit) && dem) {
			fy_emit_puts(emit, fyewt_document_indicator, "...");
			fy_emit_putc(emit, fyewt_linebreak, '\n');
			emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
			emit->flags |= FYEF_HAD_DOCUMENT_END;
		} else
			emit->flags &= ~FYEF_HAD_DOCUMENT_END;
	} else {
		if (!fy_emit_is_json_mode(emit) && dem) {
			if (emit->column != 0) {
				fy_emit_putc(emit, fyewt_linebreak, '\n');
				emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
			}
			fy_emit_puts(emit, fyewt_document_indicator, "...");
			emit->flags &= ~(FYEF_WHITESPACE | FYEF_INDENTATION);
			emit->flags |= FYEF_HAD_DOCUMENT_END;
		} else
			emit->flags &= ~FYEF_HAD_DOCUMENT_END;
	}

	/* mark that we did output a document earlier */
	emit->flags |= FYEF_HAD_DOCUMENT_OUTPUT;

	/* stop our association with the document */
	emit->fyds = NULL;

	return 0;
}

int fy_emit_document_end(struct fy_emitter *emit)
{
	int ret;

	ret = fy_emit_common_document_end(emit, false, false);
	if (ret)
		return ret;

	emit->fyd = NULL;
	return 0;
}

int fy_emit_common_explicit_document_end(struct fy_emitter *emit)
{
	if (!emit)
		return -1;

	if (emit->column != 0) {
		fy_emit_putc(emit, fyewt_linebreak, '\n');
		emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
	}

	if (!fy_emit_is_json_mode(emit)) {
		fy_emit_puts(emit, fyewt_document_indicator, "...");
		fy_emit_putc(emit, fyewt_linebreak, '\n');
		emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
		emit->flags |= FYEF_HAD_DOCUMENT_END;
	} else
		emit->flags &= ~FYEF_HAD_DOCUMENT_END;

	/* mark that we did output a document earlier */
	emit->flags |= FYEF_HAD_DOCUMENT_OUTPUT;

	/* stop our association with the document */
	emit->fyds = NULL;

	return 0;
}

int fy_emit_explicit_document_end(struct fy_emitter *emit)
{
	int ret;

	ret = fy_emit_common_explicit_document_end(emit);
	if (ret)
		return ret;

	emit->fyd = NULL;
	return 0;
}

void fy_emit_reset(struct fy_emitter *emit, bool reset_events)
{
	struct fy_eventp *fyep;

	emit->line = 0;
	emit->column = 0;
	emit->flow_level = 0;
	emit->output_error = 0;
	/* start as if there was a previous document with an explicit end */
	/* this allows implicit documents start without an indicator */
	emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION | FYEF_HAD_DOCUMENT_END;

	emit->state = FYES_NONE;

	/* reset the accumulator */
	fy_emit_accum_reset(&emit->ea);

	/* streaming mode indent */
	emit->s_indent = -1;
	/* streaming mode flags */
	emit->s_flags = DDNF_ROOT;

	emit->state_stack_top = 0;
	emit->sc_stack_top = 0;

	/* and release any queued events */
	if (reset_events) {
		while ((fyep = fy_eventp_list_pop(&emit->queued_events)) != NULL)
			fy_eventp_release(fyep);
	}
}

int fy_emit_setup(struct fy_emitter *emit, const struct fy_emitter_cfg *cfg)
{
	struct fy_diag *diag;

	if (!cfg)
		return -1;

	memset(emit, 0, sizeof(*emit));

	emit->cfg = *cfg;
	if (!emit->cfg.output)
		emit->cfg.output = fy_emitter_default_output;

	diag = cfg->diag;

	if (!diag) {
		diag = fy_diag_create(NULL);
		if (!diag)
			return -1;
	} else
		fy_diag_ref(diag);

	emit->diag = diag;

	fy_emit_accum_init(&emit->ea, emit->ea_inplace_buf, sizeof(emit->ea_inplace_buf), 0, fylb_cr_nl);
	fy_eventp_list_init(&emit->queued_events);

	emit->state_stack = emit->state_stack_inplace;
	emit->state_stack_alloc = sizeof(emit->state_stack_inplace)/sizeof(emit->state_stack_inplace[0]);

	emit->sc_stack = emit->sc_stack_inplace;
	emit->sc_stack_alloc = sizeof(emit->sc_stack_inplace)/sizeof(emit->sc_stack_inplace[0]);

	fy_eventp_list_init(&emit->recycled_eventp);
	fy_token_list_init(&emit->recycled_token);

	/* suppress recycling if we must */
	emit->suppress_recycling_force = getenv("FY_VALGRIND") && !getenv("FY_VALGRIND_RECYCLING");
	emit->suppress_recycling = emit->suppress_recycling_force;

	if (!emit->suppress_recycling) {
		emit->recycled_eventp_list = &emit->recycled_eventp;
		emit->recycled_token_list = &emit->recycled_token;
	} else {
		emit->recycled_eventp_list = NULL;
		emit->recycled_token_list = NULL;
	}

	fy_emit_reset(emit, false);

	return 0;
}

void fy_emit_cleanup(struct fy_emitter *emit)
{
	struct fy_eventp *fyep;
	struct fy_token *fyt;

	/* call the finalizer if it exists */
	if (emit->finalizer)
		emit->finalizer(emit);

	while ((fyt = fy_token_list_pop(&emit->recycled_token)) != NULL)
		fy_token_free(fyt);

	while ((fyep = fy_eventp_list_pop(&emit->recycled_eventp)) != NULL)
		fy_eventp_free(fyep);

	if (!emit->fyd && emit->fyds)
		fy_document_state_unref(emit->fyds);

	fy_emit_accum_cleanup(&emit->ea);

	while ((fyep = fy_eventp_list_pop(&emit->queued_events)) != NULL)
		fy_eventp_release(fyep);

	if (emit->state_stack && emit->state_stack != emit->state_stack_inplace)
		free(emit->state_stack);

	if (emit->sc_stack && emit->sc_stack != emit->sc_stack_inplace)
		free(emit->sc_stack);

	fy_diag_unref(emit->diag);
}

int fy_emit_node_no_check(struct fy_emitter *emit, struct fy_node *fyn)
{
	if (fyn)
		fy_emit_node_internal(emit, fyn, DDNF_ROOT, -1, false);
	return 0;
}

int fy_emit_node(struct fy_emitter *emit, struct fy_node *fyn)
{
	int ret;

	ret = fy_emit_node_check(emit, fyn);
	if (ret)
		return ret;

	return fy_emit_node_no_check(emit, fyn);
}

int fy_emit_root_node_no_check(struct fy_emitter *emit, struct fy_node *fyn)
{
	if (!emit || !fyn)
		return -1;

	/* top comment first */
	fy_emit_node_comment(emit, fyn, DDNF_ROOT, -1, fycp_top);

	fy_emit_node_internal(emit, fyn, DDNF_ROOT, -1, false);

	/* right comment next */
	fy_emit_node_comment(emit, fyn, DDNF_ROOT, -1, fycp_right);

	/* bottom comment last */
	fy_emit_node_comment(emit, fyn, DDNF_ROOT, -1, fycp_bottom);

	return 0;
}

int fy_emit_root_node(struct fy_emitter *emit, struct fy_node *fyn)
{
	int ret;

	if (!emit || !fyn)
		return -1;

	ret = fy_emit_node_check(emit, fyn);
	if (ret)
		return ret;

	return fy_emit_root_node_no_check(emit, fyn);
}

void fy_emit_prepare_document_state(struct fy_emitter *emit, struct fy_document_state *fyds)
{
	if (!emit || !fyds)
		return;

	/* if the original document was JSON and the mode is ORIGINAL turn on JSON mode */
	emit->source_json = fyds && fyds->json_mode;
	emit->force_json = (emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK)) == FYECF_MODE_ORIGINAL &&
			   emit->source_json;
}

int fy_emit_document_no_check(struct fy_emitter *emit, struct fy_document *fyd)
{
	int rc;

	rc = fy_emit_document_start(emit, fyd, NULL);
	if (rc)
		return rc;

	rc = fy_emit_root_node_no_check(emit, fyd->root);
	if (rc)
		return rc;

	rc = fy_emit_document_end(emit);

	return rc;
}

int fy_emit_document(struct fy_emitter *emit, struct fy_document *fyd)
{
	int ret;

	if (!emit)
		return -1;

	if (fyd) {
		fy_emit_prepare_document_state(emit, fyd->fyds);

		if (fyd->root) {
			ret = fy_emit_node_check(emit, fyd->root);
			if (ret)
				return ret;
		}
	}

	return fy_emit_document_no_check(emit, fyd);
}

struct fy_emitter *fy_emitter_create(const struct fy_emitter_cfg *cfg)
{
	struct fy_emitter *emit;
	int rc;

	if (!cfg)
		return NULL;

	emit = malloc(sizeof(*emit));
	if (!emit)
		return NULL;

	rc = fy_emit_setup(emit, cfg);
	if (rc) {
		free(emit);
		return NULL;
	}

	return emit;
}

void fy_emitter_destroy(struct fy_emitter *emit)
{
	if (!emit)
		return;

	fy_emit_cleanup(emit);

	free(emit);
}

const struct fy_emitter_cfg *fy_emitter_get_cfg(struct fy_emitter *emit)
{
	if (!emit)
		return NULL;

	return &emit->cfg;
}

struct fy_diag *fy_emitter_get_diag(struct fy_emitter *emit)
{
	if (!emit || !emit->diag)
		return NULL;
	return fy_diag_ref(emit->diag);
}

int fy_emitter_set_diag(struct fy_emitter *emit, struct fy_diag *diag)
{
	struct fy_diag_cfg dcfg;

	if (!emit)
		return -1;

	/* default? */
	if (!diag) {
		fy_diag_cfg_default(&dcfg);
		diag = fy_diag_create(&dcfg);
		if (!diag)
			return -1;
	}

	fy_diag_unref(emit->diag);
	emit->diag = fy_diag_ref(diag);

	return 0;
}

void fy_emitter_set_finalizer(struct fy_emitter *emit,
		void (*finalizer)(struct fy_emitter *emit))
{
	if (!emit)
		return;
	emit->finalizer = finalizer;
}

struct fy_emit_buffer_state {
	char **bufp;
	size_t *sizep;
	char *buf;
	size_t size;
	size_t pos;
	size_t need;
	bool allocate_buffer;
};

static int do_buffer_output(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *str, int leni, void *userdata)
{
	struct fy_emit_buffer_state *state = emit->cfg.userdata;
	size_t left, pagesize, size, len;
	char *bufnew;

	/* convert to unsigned and use that */
	len = (size_t)leni;

	/* no funky business */
	if (len < 0)
		return -1;

	state->need += len;
	left = state->size - state->pos;
	if (left < len) {
		if (!state->allocate_buffer)
			return 0;

		pagesize = fy_get_pagesize();
		size = state->need + pagesize - 1;
		size = size - size % pagesize;

		bufnew = realloc(state->buf, size);
		if (!bufnew)
			return -1;
		state->buf = bufnew;
		state->size = size;
		left = state->size - state->pos;

	}

	if (len > left)
		len = left;
	if (state->buf)
		memcpy(state->buf + state->pos, str, len);
	state->pos += len;

	return len;
}

static void
fy_emitter_str_finalizer(struct fy_emitter *emit)
{
	struct fy_emit_buffer_state *state;

	if (!emit || !(state = emit->cfg.userdata))
		return;

	/* if the buffer is allowed to allocate_buffer... */
	if (state->allocate_buffer && state->buf)
		free(state->buf);
	free(state);

	emit->cfg.userdata = NULL;
}

static struct fy_emitter *
fy_emitter_create_str_internal(enum fy_emitter_cfg_flags flags, char **bufp, size_t *sizep, bool allocate_buffer)
{
	struct fy_emitter *emit;
	struct fy_emitter_cfg emit_cfg;
	struct fy_emit_buffer_state *state;

	state = malloc(sizeof(*state));
	if (!state)
		return NULL;

	/* if any of these NULL, it's a allocation case */
	if ((!bufp || !sizep) && !allocate_buffer)
		return NULL;

	if (bufp && sizep) {
		state->bufp = bufp;
		state->buf = *bufp;
		state->sizep = sizep;
		state->size = *sizep;
	} else {
		state->bufp = NULL;
		state->buf = NULL;
		state->sizep = NULL;
		state->size = 0;
	}
	state->pos = 0;
	state->need = 0;
	state->allocate_buffer = allocate_buffer;

	memset(&emit_cfg, 0, sizeof(emit_cfg));
	emit_cfg.output = do_buffer_output;
	emit_cfg.userdata = state;
	emit_cfg.flags = flags;

	emit = fy_emitter_create(&emit_cfg);
	if (!emit)
		goto err_out;

	/* set finalizer to cleanup */
	fy_emitter_set_finalizer(emit, fy_emitter_str_finalizer);

	return emit;

err_out:
	if (state)
		free(state);
	return NULL;
}

static int
fy_emitter_collect_str_internal(struct fy_emitter *emit, char **bufp, size_t *sizep)
{
	struct fy_emit_buffer_state *state;
	char *buf;
	int rc;

	state = emit->cfg.userdata;
	assert(state);

	/* if NULL, then use the values stored on the state */
	if (!bufp)
		bufp = state->bufp;
	if (!sizep)
		sizep = state->sizep;

	/* terminating zero */
	rc = do_buffer_output(emit, fyewt_terminating_zero, "\0", 1, state);
	if (rc != 1)
		goto err_out;

	state->size = state->need;

	if (state->allocate_buffer) {
		/* resize */
		buf = realloc(state->buf, state->size);
		/* very likely since we shrink the buffer, but make sure we don't error out */
		if (buf)
			state->buf = buf;
	}

	/* retreive the buffer and size */
	*sizep = state->size;
	*bufp = state->buf;

	/* reset the buffer, ownership now to the caller */
	state->buf = NULL;
	state->size = 0;
	state->pos = 0;
	state->bufp = NULL;
	state->sizep = NULL;

	return 0;

err_out:
	*bufp = NULL;
	*sizep = 0;
	return -1;
}

static int fy_emit_str_internal(struct fy_document *fyd,
				enum fy_emitter_cfg_flags flags,
				struct fy_node *fyn, char **bufp, size_t *sizep,
				bool allocate_buffer)
{
	struct fy_emitter *emit = NULL;
	int rc = -1;

	emit = fy_emitter_create_str_internal(flags, bufp, sizep, allocate_buffer);
	if (!emit)
		goto out_err;

	if (fyd) {
		fy_emit_prepare_document_state(emit, fyd->fyds);
		rc = 0;
		if (fyd->root)
			rc = fy_emit_node_check(emit, fyd->root);
		if (!rc)
			rc = fy_emit_document_no_check(emit, fyd);
	} else {
		rc = fy_emit_node_check(emit, fyn);
		if (!rc)
			rc = fy_emit_node_no_check(emit, fyn);
	}

	if (rc)
		goto out_err;

	rc = fy_emitter_collect_str_internal(emit, NULL, NULL);
	if (rc)
		goto out_err;

	/* OK, all done */

out_err:
	fy_emitter_destroy(emit);
	return rc;
}

int fy_emit_document_to_buffer(struct fy_document *fyd, enum fy_emitter_cfg_flags flags, char *buf, size_t size)
{
	int rc;

	rc = fy_emit_str_internal(fyd, flags, NULL, &buf, &size, false);
	if (rc != 0)
		return -1;
	return size;
}

char *fy_emit_document_to_string(struct fy_document *fyd, enum fy_emitter_cfg_flags flags)
{
	char *buf;
	size_t size;
	int rc;

	buf = NULL;
	size = 0;
	rc = fy_emit_str_internal(fyd, flags, NULL, &buf, &size, true);
	if (rc != 0)
		return NULL;
	return buf;
}

struct fy_emitter *
fy_emit_to_buffer(enum fy_emitter_cfg_flags flags, char *buf, size_t size)
{
	if (!buf)
		return NULL;

	return fy_emitter_create_str_internal(flags, &buf, &size, false);
}

char *
fy_emit_to_buffer_collect(struct fy_emitter *emit, size_t *sizep)
{
	int rc;
	char *buf;

	if (!emit || !sizep)
		return NULL;

	rc = fy_emitter_collect_str_internal(emit, &buf, sizep);
	if (rc) {
		*sizep = 0;
		return NULL;
	}
	return buf;
}

struct fy_emitter *
fy_emit_to_string(enum fy_emitter_cfg_flags flags)
{
	return fy_emitter_create_str_internal(flags, NULL, NULL, true);
}

char *
fy_emit_to_string_collect(struct fy_emitter *emit, size_t *sizep)
{
	int rc;
	char *buf;

	if (!emit || !sizep)
		return NULL;

	rc = fy_emitter_collect_str_internal(emit, &buf, sizep);
	if (rc) {
		*sizep = 0;
		return NULL;
	}
	return buf;
}

static int do_file_output(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *str, int leni, void *userdata)
{
	FILE *fp = userdata;
	size_t len;

	len = (size_t)leni;

	/* no funky stuff */
	if (len < 0)
		return -1;

	return fwrite(str, 1, len, fp);
}

int fy_emit_document_to_fp(struct fy_document *fyd, enum fy_emitter_cfg_flags flags,
			   FILE *fp)
{
	struct fy_emitter emit_state, *emit = &emit_state;
	struct fy_emitter_cfg emit_cfg;
	int rc;

	if (!fp)
		return -1;

	memset(&emit_cfg, 0, sizeof(emit_cfg));
	emit_cfg.output = do_file_output;
	emit_cfg.userdata = fp;
	emit_cfg.flags = flags;
	fy_emit_setup(emit, &emit_cfg);

	fy_emit_prepare_document_state(emit, fyd->fyds);

	rc = 0;
	if (fyd->root)
		rc = fy_emit_node_check(emit, fyd->root);

	rc = fy_emit_document_no_check(emit, fyd);

	fy_emit_cleanup(emit);

	return rc ? rc : 0;
}

int fy_emit_document_to_file(struct fy_document *fyd,
			     enum fy_emitter_cfg_flags flags,
			     const char *filename)
{
	FILE *fp;
	int rc;

	fp = filename ? fopen(filename, "wa") : stdout;
	if (!fp)
		return -1;

	rc = fy_emit_document_to_fp(fyd, flags, fp);

	if (fp != stdout)
		fclose(fp);

	return rc ? rc : 0;
}

static int do_fd_output(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *str, int leni, void *userdata)
{
	size_t len;
	int fd;
	ssize_t wrn;
	int total;

	len = (size_t)leni;

	/* no funky stuff */
	if (len < 0)
		return -1;

	/* get the file descriptor */
	fd = (int)(uintptr_t)userdata;
	if (fd < 0)
		return -1;

	/* loop output to fd */
	total = 0;
	while (len > 0) {

		do {
			wrn = write(fd, str, len);
		} while (wrn == -1 && errno == EAGAIN);

		if (wrn == -1)
			return -1;

		if (wrn == 0)
			return total;

		len -= wrn;
		str += wrn;
		total += wrn;
	}

	return total;
}

int fy_emit_document_to_fd(struct fy_document *fyd, enum fy_emitter_cfg_flags flags, int fd)
{
	struct fy_emitter emit_state, *emit = &emit_state;
	struct fy_emitter_cfg emit_cfg;
	int rc;

	if (fd < 0)
		return -1;

	memset(&emit_cfg, 0, sizeof(emit_cfg));
	emit_cfg.output = do_fd_output;
	emit_cfg.userdata = (void *)(uintptr_t)fd;
	emit_cfg.flags = flags;
	fy_emit_setup(emit, &emit_cfg);

	fy_emit_prepare_document_state(emit, fyd->fyds);

	rc = 0;
	if (fyd->root)
		rc = fy_emit_node_check(emit, fyd->root);

	rc = fy_emit_document_no_check(emit, fyd);

	fy_emit_cleanup(emit);

	return rc ? rc : 0;
}

int fy_emit_node_to_buffer(struct fy_node *fyn, enum fy_emitter_cfg_flags flags, char *buf, size_t size)
{
	int rc;

	rc = fy_emit_str_internal(NULL, flags, fyn, &buf, &size, false);
	if (rc != 0)
		return -1;
	return size;
}

char *fy_emit_node_to_string(struct fy_node *fyn, enum fy_emitter_cfg_flags flags)
{
	char *buf;
	size_t size;
	int rc;

	buf = NULL;
	size = 0;
	rc = fy_emit_str_internal(NULL, flags, fyn, &buf, &size, true);
	if (rc != 0)
		return NULL;
	return buf;
}

static bool fy_emit_ready(struct fy_emitter *emit)
{
	struct fy_eventp *fyep;
	int need, count, level;

	/* no events in the list, not ready */
	fyep = fy_eventp_list_head(&emit->queued_events);
	if (!fyep)
		return false;

	/* some events need more than one */
	switch (fyep->e.type) {
	case FYET_DOCUMENT_START:
		need = 1;
		break;
	case FYET_SEQUENCE_START:
		need = 2;
		break;
	case FYET_MAPPING_START:
		need = 3;
		break;
	default:
		need = 0;
		break;
	}

	/* if we don't need any more, that's enough */
	if (!need)
		return true;

	level = 0;
	count = 0;
	for (; fyep; fyep = fy_eventp_next(&emit->queued_events, fyep)) {
		count++;

		if (count > need)
			return true;

		switch (fyep->e.type) {
		case FYET_STREAM_START:
		case FYET_DOCUMENT_START:
		case FYET_SEQUENCE_START:
		case FYET_MAPPING_START:
			level++;
			break;
		case FYET_STREAM_END:
		case FYET_DOCUMENT_END:
		case FYET_SEQUENCE_END:
		case FYET_MAPPING_END:
			level--;
			break;
		default:
			break;
		}

		if (!level)
			return true;
	}

	return false;
}

extern const char *fy_event_type_txt[];

const char *fy_emitter_state_txt[] = {
	[FYES_NONE]			= "NONE",
	[FYES_STREAM_START]		= "STREAM_START",
	[FYES_FIRST_DOCUMENT_START]	= "FIRST_DOCUMENT_START",
	[FYES_DOCUMENT_START]		= "DOCUMENT_START",
	[FYES_DOCUMENT_CONTENT]		= "DOCUMENT_CONTENT",
	[FYES_DOCUMENT_END]		= "DOCUMENT_END",
	[FYES_SEQUENCE_FIRST_ITEM]	= "SEQUENCE_FIRST_ITEM",
	[FYES_SEQUENCE_ITEM]		= "SEQUENCE_ITEM",
	[FYES_MAPPING_FIRST_KEY]	= "MAPPING_FIRST_KEY",
	[FYES_MAPPING_KEY]		= "MAPPING_KEY",
	[FYES_MAPPING_SIMPLE_VALUE]	= "MAPPING_SIMPLE_VALUE",
	[FYES_MAPPING_VALUE]		= "MAPPING_VALUE",
	[FYES_END]			= "END",
};

struct fy_eventp *
fy_emit_next_event(struct fy_emitter *emit)
{
	if (!fy_emit_ready(emit))
		return NULL;

	return fy_eventp_list_pop(&emit->queued_events);
}

struct fy_eventp *
fy_emit_peek_next_event(struct fy_emitter *emit)
{
	if (!fy_emit_ready(emit))
		return NULL;

	return fy_eventp_list_head(&emit->queued_events);
}

bool fy_emit_streaming_sequence_empty(struct fy_emitter *emit)
{
	enum fy_emitter_cfg_flags flags = emit->cfg.flags & FYECF_MODE(FYECF_MODE_MASK);
	struct fy_eventp *fyepn;
	struct fy_event *fyen;

	if (flags == FYECF_MODE_BLOCK || flags == FYECF_MODE_DEJSON)
		return false;

	fyepn = fy_emit_peek_next_event(emit);
	fyen = fyepn ? &fyepn->e : NULL;

	return !fyen || fyen->type == FYET_SEQUENCE_END;
}

bool fy_emit_streaming_mapping_empty(struct fy_emitter *emit)
{
	struct fy_eventp *fyepn;
	struct fy_event *fyen;

	fyepn = fy_emit_peek_next_event(emit);
	fyen = fyepn ? &fyepn->e : NULL;

	return !fyen || fyen->type == FYET_MAPPING_END;
}

static void fy_emit_goto_state(struct fy_emitter *emit, enum fy_emitter_state state)
{
	if (emit->state == state)
		return;

	emit->state = state;
}

static int fy_emit_push_state(struct fy_emitter *emit, enum fy_emitter_state state)
{
	enum fy_emitter_state *states;

	if (emit->state_stack_top >= emit->state_stack_alloc) {
		states = realloc(emit->state_stack == emit->state_stack_inplace ? NULL : emit->state_stack,
				sizeof(emit->state_stack[0]) * emit->state_stack_alloc * 2);
		if (!states)
			return -1;

		if (emit->state_stack == emit->state_stack_inplace)
			memcpy(states, emit->state_stack, sizeof(emit->state_stack[0]) * emit->state_stack_top);
		emit->state_stack = states;
		emit->state_stack_alloc *= 2;
	}
	emit->state_stack[emit->state_stack_top++] = state;

	return 0;
}

enum fy_emitter_state fy_emit_pop_state(struct fy_emitter *emit)
{
	if (!emit->state_stack_top)
		return FYES_NONE;

	return emit->state_stack[--emit->state_stack_top];
}

int fy_emit_push_sc(struct fy_emitter *emit, struct fy_emit_save_ctx *sc)
{
	struct fy_emit_save_ctx *scs;

	if (emit->sc_stack_top >= emit->sc_stack_alloc) {
		scs = realloc(emit->sc_stack == emit->sc_stack_inplace ? NULL : emit->sc_stack,
				sizeof(emit->sc_stack[0]) * emit->sc_stack_alloc * 2);
		if (!scs)
			return -1;

		if (emit->sc_stack == emit->sc_stack_inplace)
			memcpy(scs, emit->sc_stack, sizeof(emit->sc_stack[0]) * emit->sc_stack_top);
		emit->sc_stack = scs;
		emit->sc_stack_alloc *= 2;
	}
	emit->sc_stack[emit->sc_stack_top++] = *sc;

	return 0;
}

int fy_emit_pop_sc(struct fy_emitter *emit, struct fy_emit_save_ctx *sc)
{
	if (!emit->sc_stack_top)
		return -1;

	*sc = emit->sc_stack[--emit->sc_stack_top];

	return 0;
}

static int fy_emit_streaming_node(struct fy_emitter *emit, struct fy_eventp *fyep, int flags)
{
	struct fy_event *fye = &fyep->e;
	struct fy_emit_save_ctx *sc = &emit->s_sc;
	enum fy_node_style style, xstyle;
	int ret, s_flags, s_indent;

	if (fye->type != FYET_ALIAS && fye->type != FYET_SCALAR &&
			(emit->s_flags & DDNF_ROOT) && emit->column != 0) {
		fy_emit_putc(emit, fyewt_linebreak, '\n');
		emit->flags = FYEF_WHITESPACE | FYEF_INDENTATION;
	}

	emit->s_flags = flags;

	switch (fye->type) {
	case FYET_ALIAS:
		fy_emit_token_write_alias(emit, fye->alias.anchor, emit->s_flags, emit->s_indent);
		fy_emit_goto_state(emit, fy_emit_pop_state(emit));
		break;

	case FYET_SCALAR:
		/* if we're pretty and at column 0 (meaning it's a single scalar document) output --- */
		if ((emit->s_flags & DDNF_ROOT) && fy_emit_is_pretty_mode(emit) && !emit->column &&
				!fy_emit_is_flow_mode(emit) && !(emit->s_flags & DDNF_FLOW))
			fy_emit_document_start_indicator(emit);
		fy_emit_common_node_preamble(emit, fye->scalar.anchor, fye->scalar.tag, emit->s_flags, emit->s_indent);
		style = fye->scalar.value ?
				fy_node_style_from_scalar_style(fye->scalar.value->scalar.style) :
				FYNS_PLAIN;
		fy_emit_token_scalar(emit, fye->scalar.value, emit->s_flags, emit->s_indent, style, fye->scalar.tag);
		fy_emit_goto_state(emit, fy_emit_pop_state(emit));
		break;

	case FYET_SEQUENCE_START:

		/* save this context */
		ret = fy_emit_push_sc(emit, sc);
		if (ret)
			return ret;

		s_flags = emit->s_flags;
		s_indent = emit->s_indent;

		xstyle = fye->sequence_start.sequence_start->type == FYTT_BLOCK_SEQUENCE_START ?
			FYNS_BLOCK : FYNS_FLOW;

		fy_emit_common_node_preamble(emit, fye->sequence_start.anchor, fye->sequence_start.tag, emit->s_flags, emit->s_indent);

		/* create new context */
		memset(sc, 0, sizeof(*sc));
		sc->flags = emit->s_flags & (DDNF_ROOT | DDNF_SEQ | DDNF_MAP);
		sc->indent = emit->s_indent;
		sc->empty = fy_emit_streaming_sequence_empty(emit);
		sc->flow_token = xstyle == FYNS_FLOW;
		sc->flow = !!(s_flags & DDNF_FLOW);
		sc->xstyle = xstyle;
		sc->old_indent = sc->indent;
		sc->s_flags = s_flags;
		sc->s_indent = s_indent;
		sc->s_flags = s_flags;
		sc->s_indent = s_indent;

		fy_emit_sequence_prolog(emit, sc);
		sc->flags &= ~DDNF_MAP;
		sc->flags |= DDNF_SEQ;

		emit->s_flags = sc->flags;
		emit->s_indent = sc->indent;

		fy_emit_goto_state(emit, FYES_SEQUENCE_FIRST_ITEM);
		break;

	case FYET_MAPPING_START:
		/* save this context */
		ret = fy_emit_push_sc(emit, sc);
		if (ret)
			return ret;

		s_flags = emit->s_flags;
		s_indent = emit->s_indent;

		xstyle = fye->mapping_start.mapping_start->type == FYTT_BLOCK_MAPPING_START ?
			FYNS_BLOCK : FYNS_FLOW;

		fy_emit_common_node_preamble(emit, fye->mapping_start.anchor, fye->mapping_start.tag, emit->s_flags, emit->s_indent);

		/* create new context */
		memset(sc, 0, sizeof(*sc));
		sc->flags = emit->s_flags & (DDNF_ROOT | DDNF_SEQ | DDNF_MAP);
		sc->indent = emit->s_indent;
		sc->empty = fy_emit_streaming_mapping_empty(emit);
		sc->flow_token = xstyle == FYNS_FLOW;
		sc->flow = !!(s_flags & DDNF_FLOW);
		sc->xstyle = xstyle;
		sc->old_indent = sc->indent;
		sc->s_flags = s_flags;
		sc->s_indent = s_indent;
		sc->s_flags = s_flags;
		sc->s_indent = s_indent;

		fy_emit_mapping_prolog(emit, sc);
		sc->flags &= ~DDNF_SEQ;
		sc->flags |= DDNF_MAP;

		emit->s_flags = sc->flags;
		emit->s_indent = sc->indent;

		fy_emit_goto_state(emit, FYES_MAPPING_FIRST_KEY);
		break;

	default:
		fy_error(emit->diag, "%s: expected ALIAS|SCALAR|SEQUENCE_START|MAPPING_START", __func__);
		return -1;
	}

	return 0;
}

static int fy_emit_handle_stream_start(struct fy_emitter *emit, struct fy_eventp *fyep)
{
	struct fy_event *fye = &fyep->e;

	if (fye->type != FYET_STREAM_START) {
		fy_error(emit->diag, "%s: expected FYET_STREAM_START", __func__);
		return -1;
	}

	fy_emit_reset(emit, false);

	fy_emit_goto_state(emit, FYES_FIRST_DOCUMENT_START);

	return 0;
}

static int fy_emit_handle_document_start(struct fy_emitter *emit, struct fy_eventp *fyep, bool first)
{
	struct fy_event *fye = &fyep->e;
	struct fy_document_state *fyds;

	if (fye->type != FYET_DOCUMENT_START &&
	    fye->type != FYET_STREAM_END) {
		fy_error(emit->diag, "%s: expected FYET_DOCUMENT_START|FYET_STREAM_END", __func__);
		return -1;
	}

	if (fye->type == FYET_STREAM_END) {
		fy_emit_goto_state(emit, FYES_END);
		return 0;
	}

	/* transfer ownership to the emitter */
	fyds = fye->document_start.document_state;
	fye->document_start.document_state = NULL;

	/* prepare (i.e. adapt to the document state) */
	fy_emit_prepare_document_state(emit, fyds);

	fy_emit_common_document_start(emit, fyds, false);

	fy_emit_goto_state(emit, FYES_DOCUMENT_CONTENT);

	return 0;
}

static int fy_emit_handle_document_end(struct fy_emitter *emit, struct fy_eventp *fyep)
{
	struct fy_document_state *fyds;
	struct fy_event *fye = &fyep->e;
	int ret;

	if (fye->type != FYET_DOCUMENT_END) {
		fy_error(emit->diag, "%s: expected FYET_DOCUMENT_END", __func__);
		return -1;
	}

	fyds = emit->fyds;

	ret = fy_emit_common_document_end(emit, true, fye->document_end.implicit);
	if (ret)
		return ret;

	fy_document_state_unref(fyds);

	fy_emit_reset(emit, false);
	fy_emit_goto_state(emit, FYES_DOCUMENT_START);
	return 0;
}

static int fy_emit_handle_document_content(struct fy_emitter *emit, struct fy_eventp *fyep)
{
	struct fy_event *fye = &fyep->e;
	int ret;

	/* empty document? */
	if (fye->type == FYET_DOCUMENT_END)
		return fy_emit_handle_document_end(emit, fyep);

	ret = fy_emit_push_state(emit, FYES_DOCUMENT_END);
	if (ret)
		return ret;

	return fy_emit_streaming_node(emit, fyep, DDNF_ROOT);
}

static int fy_emit_handle_sequence_item(struct fy_emitter *emit, struct fy_eventp *fyep, bool first)
{
	struct fy_event *fye = &fyep->e;
	struct fy_emit_save_ctx *sc = &emit->s_sc;
	struct fy_token *fyt_item = NULL;
	int ret;

	fy_token_unref_rl(emit->recycled_token_list, sc->fyt_last_value);
	sc->fyt_last_value = NULL;

	switch (fye->type) {
	case FYET_SEQUENCE_END:
		fy_emit_sequence_item_epilog(emit, sc, true, sc->fyt_last_value);

		/* emit epilog */
		fy_emit_sequence_epilog(emit, sc);
		/* pop state */
		ret = fy_emit_pop_sc(emit, sc);
		/* pop state */
		fy_emit_goto_state(emit, fy_emit_pop_state(emit));

		/* restore indent and flags */
		emit->s_indent = sc->s_indent;
		emit->s_flags = sc->s_flags;
		return ret;

	case FYET_ALIAS:
		fyt_item = fye->alias.anchor;
		break;
	case FYET_SCALAR:
		fyt_item = fye->scalar.value;
		break;
	case FYET_SEQUENCE_START:
		fyt_item = fye->sequence_start.sequence_start;
		break;
	case FYET_MAPPING_START:
		fyt_item = fye->mapping_start.mapping_start;
		break;
	default:
		fy_error(emit->diag, "%s: expected SEQUENCE_END|ALIAS|SCALAR|SEQUENCE_START|MAPPING_START", __func__);
		return -1;
	}

	ret = fy_emit_push_state(emit, FYES_SEQUENCE_ITEM);
	if (ret)
		return ret;

	/* reset indent and flags for each item */
	emit->s_indent = sc->indent;
	emit->s_flags = sc->flags;

	if (!first)
		fy_emit_sequence_item_epilog(emit, sc, false, sc->fyt_last_value);

	sc->fyt_last_value = fyt_item;

	fy_emit_sequence_item_prolog(emit, sc, fyt_item);

	ret = fy_emit_streaming_node(emit, fyep, sc->flags);

	switch (fye->type) {
	case FYET_ALIAS:
		fye->alias.anchor = NULL;	/* take ownership */
		break;
	case FYET_SCALAR:
		fye->scalar.value = NULL;	/* take ownership */
		break;
	case FYET_SEQUENCE_START:
		fye->sequence_start.sequence_start = NULL;	/* take ownership */
		break;
	case FYET_MAPPING_START:
		fye->mapping_start.mapping_start = NULL;	/* take ownership */
		break;
	default:
		break;
	}

	return ret;
}

static int fy_emit_handle_mapping_key(struct fy_emitter *emit, struct fy_eventp *fyep, bool first)
{
	struct fy_event *fye = &fyep->e;
	struct fy_emit_save_ctx *sc = &emit->s_sc;
	struct fy_token *fyt_key = NULL;
	int ret, aflags;
	bool simple_key;

	fy_token_unref_rl(emit->recycled_token_list, sc->fyt_last_key);
	sc->fyt_last_key = NULL;
	fy_token_unref_rl(emit->recycled_token_list, sc->fyt_last_value);
	sc->fyt_last_value = NULL;

	simple_key = false;

	switch (fye->type) {
	case FYET_MAPPING_END:
		fy_emit_mapping_value_epilog(emit, sc, true, sc->fyt_last_value);

		/* emit epilog */
		fy_emit_mapping_epilog(emit, sc);
		/* pop state */
		ret = fy_emit_pop_sc(emit, sc);
		/* pop state */
		fy_emit_goto_state(emit, fy_emit_pop_state(emit));

		/* restore indent and flags */
		emit->s_indent = sc->s_indent;
		emit->s_flags = sc->s_flags;
		return ret;

	case FYET_ALIAS:
		fyt_key = fye->alias.anchor;
		simple_key = true;
		break;
	case FYET_SCALAR:
		fyt_key = fye->scalar.value;
		aflags = fy_token_text_analyze(fyt_key);
		simple_key = !!(aflags & FYTTAF_CAN_BE_SIMPLE_KEY);
		break;
	case FYET_SEQUENCE_START:
		fyt_key = fye->sequence_start.sequence_start;
		simple_key = fy_emit_streaming_sequence_empty(emit);
		break;
	case FYET_MAPPING_START:
		fyt_key = fye->mapping_start.mapping_start;
		simple_key = fy_emit_streaming_mapping_empty(emit);
		break;
	default:
		fy_error(emit->diag, "%s: expected MAPPING_END|ALIAS|SCALAR|SEQUENCE_START|MAPPING_START", __func__);
		return -1;
	}

	ret = fy_emit_push_state(emit, FYES_MAPPING_VALUE);
	if (ret)
		return ret;

	/* reset indent and flags for each key/value pair */
	emit->s_indent = sc->indent;
	emit->s_flags = sc->flags;

	if (!first)
		fy_emit_mapping_value_epilog(emit, sc, false, sc->fyt_last_value);

	sc->fyt_last_key = fyt_key;

	fy_emit_mapping_key_prolog(emit, sc, fyt_key, simple_key);

	ret = fy_emit_streaming_node(emit, fyep, sc->flags);

	switch (fye->type) {
	case FYET_ALIAS:
		fye->alias.anchor = NULL;	/* take ownership */
		break;
	case FYET_SCALAR:
		fye->scalar.value = NULL;	/* take ownership */
		break;
	case FYET_SEQUENCE_START:
		fye->sequence_start.sequence_start = NULL;	/* take ownership */
		break;
	case FYET_MAPPING_START:
		fye->mapping_start.mapping_start = NULL;	/* take ownership */
		break;
	default:
		break;
	}

	return ret;
}

static int fy_emit_handle_mapping_value(struct fy_emitter *emit, struct fy_eventp *fyep, bool simple)
{
	struct fy_event *fye = &fyep->e;
	struct fy_emit_save_ctx *sc = &emit->s_sc;
	struct fy_token *fyt_value = NULL;
	int ret;

	switch (fye->type) {
	case FYET_ALIAS:
		fyt_value = fye->alias.anchor;
		break;
	case FYET_SCALAR:
		fyt_value = fye->scalar.value;	/* take ownership */
		break;
	case FYET_SEQUENCE_START:
		fyt_value = fye->sequence_start.sequence_start;
		break;
	case FYET_MAPPING_START:
		fyt_value = fye->mapping_start.mapping_start;
		break;
	default:
		fy_error(emit->diag, "%s: expected ALIAS|SCALAR|SEQUENCE_START|MAPPING_START", __func__);
		return -1;
	}

	ret = fy_emit_push_state(emit, FYES_MAPPING_KEY);
	if (ret)
		return ret;

	fy_emit_mapping_key_epilog(emit, sc, sc->fyt_last_key);

	sc->fyt_last_value = fyt_value;

	fy_emit_mapping_value_prolog(emit, sc, fyt_value);

	ret = fy_emit_streaming_node(emit, fyep, sc->flags);

	switch (fye->type) {
	case FYET_ALIAS:
		fye->alias.anchor = NULL;	/* take ownership */
		break;
	case FYET_SCALAR:
		fye->scalar.value = NULL;	/* take ownership */
		break;
	case FYET_SEQUENCE_START:
		fye->sequence_start.sequence_start = NULL;	/* take ownership */
		break;
	case FYET_MAPPING_START:
		fye->mapping_start.mapping_start = NULL;	/* take ownership */
		break;
	default:
		break;
	}

	return ret;
}

int fy_emit_event_from_parser(struct fy_emitter *emit, struct fy_parser *fyp, struct fy_event *fye)
{
	struct fy_eventp *fyep;
	int ret;

	if (!emit || !fye)
		return -1;

	/* we're using the raw emitter interface, now mark first state */
	if (emit->state == FYES_NONE)
		emit->state = FYES_STREAM_START;

	fyep = fy_container_of(fye, struct fy_eventp, e);

	fy_eventp_list_add_tail(&emit->queued_events, fyep);

	ret = 0;
	while ((fyep = fy_emit_next_event(emit)) != NULL) {

		switch (emit->state) {
		case FYES_STREAM_START:
			ret = fy_emit_handle_stream_start(emit, fyep);
			break;

		case FYES_FIRST_DOCUMENT_START:
		case FYES_DOCUMENT_START:
			ret = fy_emit_handle_document_start(emit, fyep,
					emit->state == FYES_FIRST_DOCUMENT_START);
			break;

		case FYES_DOCUMENT_CONTENT:
			ret = fy_emit_handle_document_content(emit, fyep);
			break;

		case FYES_DOCUMENT_END:
			ret = fy_emit_handle_document_end(emit, fyep);
			break;

		case FYES_SEQUENCE_FIRST_ITEM:
		case FYES_SEQUENCE_ITEM:
			ret = fy_emit_handle_sequence_item(emit, fyep,
					emit->state == FYES_SEQUENCE_FIRST_ITEM);
			break;

		case FYES_MAPPING_FIRST_KEY:
		case FYES_MAPPING_KEY:
			ret = fy_emit_handle_mapping_key(emit, fyep,
					emit->state == FYES_MAPPING_FIRST_KEY);
			break;

		case FYES_MAPPING_SIMPLE_VALUE:
		case FYES_MAPPING_VALUE:
			ret = fy_emit_handle_mapping_value(emit, fyep,
					emit->state == FYES_MAPPING_SIMPLE_VALUE);
			break;

		case FYES_END:
			ret = -1;
			break;

		default:
			assert(1);      /* Invalid state. */
		}

		/* always release the event */
		if (!fyp)
			fy_eventp_release(fyep);
		else
			fy_parse_eventp_recycle(fyp, fyep);

		if (ret)
			break;
	}

	return ret;
}

int fy_emit_event(struct fy_emitter *emit, struct fy_event *fye)
{
	return fy_emit_event_from_parser(emit, NULL, fye);
}

struct fy_document_state *
fy_emitter_get_document_state(struct fy_emitter *emit)
{
	return emit ? emit->fyds : NULL;
}

int fy_emitter_default_output(struct fy_emitter *fye, enum fy_emitter_write_type type, const char *str, int len, void *userdata)
{
	struct fy_emitter_default_output_data d_local, *d;
	FILE *fp;
	int ret, w;
	const char *color = NULL;
	const char *s, *e;

	d = userdata;
	if (!d) {
		/* kinda inneficient but should not matter */
		d = &d_local;
		d->fp = stdout;
		d->colorize = isatty(STDOUT_FILENO);
		d->visible = false;
	}
	fp = d->fp;

	s = str;
	e = str + len;
	if (d->colorize) {
		switch (type) {
		case fyewt_document_indicator:
			color = "\x1b[36m";
			break;
		case fyewt_tag_directive:
		case fyewt_version_directive:
			color = "\x1b[33m";
			break;
		case fyewt_indent:
			if (d->visible) {
				fputs("\x1b[32m", fp);
				while (s < e && (w = fy_utf8_width_by_first_octet(((uint8_t)*s))) > 0) {
					/* open box - U+2423 */
					fputs("\xe2\x90\xa3", fp);
					s += w;
				}
				fputs("\x1b[0m", fp);
				return len;
			}
			break;
		case fyewt_indicator:
			if (len == 1 && (str[0] == '\'' || str[0] == '"'))
				color = "\x1b[33m";
			else if (len == 1 && str[0] == '&')
				color = "\x1b[32;1m";
			else
				color = "\x1b[35m";
			break;
		case fyewt_whitespace:
			if (d->visible) {
				fputs("\x1b[32m", fp);
				while (s < e && (w = fy_utf8_width_by_first_octet(((uint8_t)*s))) > 0) {
					/* symbol for space - U+2420 */
					/* symbol for interpunct - U+00B7 */
					fputs("\xc2\xb7", fp);
					s += w;
				}
				fputs("\x1b[0m", fp);
				return len;
			}
			break;
		case fyewt_plain_scalar:
			color = "\x1b[37;1m";
			break;
		case fyewt_single_quoted_scalar:
		case fyewt_double_quoted_scalar:
			color = "\x1b[33m";
			break;
		case fyewt_literal_scalar:
		case fyewt_folded_scalar:
			color = "\x1b[33m";
			break;
		case fyewt_anchor:
		case fyewt_tag:
		case fyewt_alias:
			color = "\x1b[32;1m";
			break;
		case fyewt_linebreak:
			if (d->visible) {
				fputs("\x1b[32m", fp);
				while (s < e && (w = fy_utf8_width_by_first_octet(((uint8_t)*s))) > 0) {
					/* symbol for space - ^M */
					/* fprintf(fp, "^M\n"); */
					/* down arrow - U+2193 */
					fputs("\xe2\x86\x93\n", fp);
					s += w;
				}
				fputs("\x1b[0m", fp);
				return len;
			}
			color = NULL;
			break;
		case fyewt_terminating_zero:
			color = NULL;
			break;
		case fyewt_plain_scalar_key:
		case fyewt_single_quoted_scalar_key:
		case fyewt_double_quoted_scalar_key:
			color = "\x1b[36;1m";
			break;
		case fyewt_comment:
			color = "\x1b[34;1m";
			break;
		}
	}

	/* don't output the terminating zero */
	if (type == fyewt_terminating_zero)
		return len;

	if (color)
		fputs(color, fp);

	ret = fwrite(str, 1, len, fp);

	if (color)
		fputs("\x1b[0m", fp);

	return ret;
}

int fy_document_default_emit_to_fp(struct fy_document *fyd, FILE *fp)
{
	struct fy_emitter emit_local, *emit = &emit_local;
	struct fy_emitter_cfg ecfg_local, *ecfg = &ecfg_local;
	struct fy_emitter_default_output_data d_local, *d = &d_local;
	int rc;

	memset(d, 0, sizeof(*d));
	d->fp = fp;
	d->colorize = isatty(fileno(fp));
	d->visible = false;

	memset(ecfg, 0, sizeof(*ecfg));
	ecfg->diag = fyd->diag;
	ecfg->userdata = d;

	rc = fy_emit_setup(emit, ecfg);
	if (rc)
		goto err_setup;

	fy_emit_prepare_document_state(emit, fyd->fyds);

	rc = 0;
	if (fyd->root)
		rc = fy_emit_node_check(emit, fyd->root);

	rc = fy_emit_document_no_check(emit, fyd);
	if (rc)
		goto err_emit;

	fy_emit_cleanup(emit);

	return 0;

err_emit:
	fy_emit_cleanup(emit);
err_setup:
	return -1;
}
