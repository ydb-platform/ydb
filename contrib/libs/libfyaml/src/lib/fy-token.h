/*
 * fy-token.h - YAML token methods header
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_TOKEN_H
#define FY_TOKEN_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-utils.h"
#include "fy-atom.h"

extern const char *fy_token_type_txt[FYTT_COUNT];

struct fy_document;
struct fy_path_expr;

static inline bool fy_token_type_is_sequence_start(enum fy_token_type type)
{
	return type == FYTT_BLOCK_SEQUENCE_START || type == FYTT_FLOW_SEQUENCE_START;
}

static inline bool fy_token_type_is_sequence_end(enum fy_token_type type)
{
	return type == FYTT_BLOCK_SEQUENCE_START || type == FYTT_FLOW_SEQUENCE_START;
}

static inline bool fy_token_type_is_sequence_marker(enum fy_token_type type)
{
	return fy_token_type_is_sequence_start(type) || fy_token_type_is_sequence_end(type);
}

static inline bool fy_token_type_is_mapping_start(enum fy_token_type type)
{
	return type == FYTT_BLOCK_MAPPING_START || type == FYTT_FLOW_MAPPING_START;
}

static inline bool fy_token_type_is_mapping_end(enum fy_token_type type)
{
	return type == FYTT_BLOCK_MAPPING_START || type == FYTT_FLOW_MAPPING_START;
}

static inline bool fy_token_type_is_mapping_marker(enum fy_token_type type)
{
	return fy_token_type_is_mapping_start(type) || fy_token_type_is_mapping_end(type);
}

/* analyze content flags */
#define FYACF_EMPTY		0x000001	/* is empty (only ws & lb) */
#define FYACF_LB		0x000002	/* has a linebreak */
#define FYACF_BLOCK_PLAIN	0x000004	/* can be a plain scalar in block context */
#define FYACF_FLOW_PLAIN	0x000008	/* can be a plain scalar in flow context */
#define FYACF_PRINTABLE		0x000010	/* every character is printable */
#define FYACF_SINGLE_QUOTED	0x000020	/* can be a single quoted scalar */
#define FYACF_DOUBLE_QUOTED	0x000040	/* can be a double quoted scalar */
#define FYACF_CONTAINS_ZERO	0x000080	/* contains a zero */
#define FYACF_DOC_IND		0x000100	/* contains document indicators */
#define FYACF_CONSECUTIVE_LB 	0x000200	/* has consecutive linebreaks */
#define FYACF_SIMPLE_KEY	0x000400	/* can be a simple key */
#define FYACF_WS		0x000800	/* has at least one whitespace */
#define FYACF_STARTS_WITH_WS	0x001000	/* starts with whitespace */
#define FYACF_STARTS_WITH_LB	0x002000	/* starts with whitespace */
#define FYACF_ENDS_WITH_WS	0x004000	/* ends with whitespace */
#define FYACF_ENDS_WITH_LB	0x008000	/* ends with linebreak */
#define FYACF_TRAILING_LB	0x010000	/* ends with trailing lb > 1 */
#define FYACF_SIZE0		0x020000	/* contains absolutely nothing */
#define FYACF_VALID_ANCHOR	0x040000	/* contains valid anchor (without & prefix) */
#define FYACF_JSON_ESCAPE	0x080000	/* contains a character that JSON escapes */

FY_TYPE_FWD_DECL_LIST(token);
struct fy_token {
	struct fy_list_head node;
	enum fy_token_type type;
	int refs;		/* when on document, we switch to reference counting */
	int analyze_flags;	/* cache of the analysis flags */
	size_t text_len;
	const char *text;
	char *text0;		/* this is allocated */
	struct fy_atom handle;
	struct fy_atom *comment;	/* only when enabled */
	union  {
		struct {
			unsigned int tag_length;	/* from start */
			unsigned int uri_length;	/* from end */
			char *prefix0;
			char *handle0;
			struct fy_tag tag;
			bool is_default;		/* true when default */
		} tag_directive;
		struct {
			enum fy_scalar_style style;
			/* path key (if requested only) */
			const char *path_key;
			size_t path_key_len;
			char *path_key_storage;	/* if this is not null, it's \0 terminated */
			bool is_null;		/* special case; the scalar was NULL */
		} scalar;
		struct {
			unsigned int skip;
			unsigned int handle_length;
			unsigned int suffix_length;
			struct fy_token *fyt_td;
			char *handle0;	/* zero terminated and allocated, only used by binding */
			char *suffix0;
			struct fy_tag tag;	/* prefix is now suffix */
		} tag;
		struct {
			struct fy_version vers;		/* parsed version number */
		} version_directive;
		/* path expressions */
		struct {
			struct fy_document *fyd;	/* when key is complex */
		} map_key;
		struct {
			int index;
		} seq_index;
		struct {
			int start_index;
			int end_index;
		} seq_slice;
		struct {
			struct fy_path_expr *expr;
		} alias;
		struct {
			int flow_level;
		} key;
	};
};
FY_TYPE_DECL_LIST(token);

static inline bool fy_token_text_is_direct(struct fy_token *fyt)
{
	if (!fyt || !fyt->text)
		return false;
	return fyt->text && fyt->text != fyt->text0;
}

void fy_token_clean_rl(struct fy_token_list *fytl, struct fy_token *fyt);
void fy_token_list_unref_all_rl(struct fy_token_list *fytl, struct fy_token_list *fytl_tofree);

static inline FY_ALWAYS_INLINE struct fy_token *
fy_token_alloc_rl(struct fy_token_list *fytl)
{
	struct fy_token *fyt;

	fyt = NULL;
	if (fytl)
		fyt = fy_token_list_pop(fytl);
	if (!fyt) {
		fyt = malloc(sizeof(*fyt));
		if (!fyt)
			return NULL;
	}

	fyt->type = FYTT_NONE;
	fyt->refs = 1;

	fyt->analyze_flags = 0;
	fyt->text_len = 0;
	fyt->text = NULL;
	fyt->text0 = NULL;
	fyt->handle.fyi = NULL;
	fyt->comment = NULL;

	return fyt;
}

static inline FY_ALWAYS_INLINE void
fy_token_free_rl(struct fy_token_list *fytl, struct fy_token *fyt)
{
	if (!fyt)
		return;

	fy_token_clean_rl(fytl, fyt);

	if (fytl)
		fy_token_list_push(fytl, fyt);
	else
		free(fyt);
}

static inline FY_ALWAYS_INLINE void
fy_token_unref_rl(struct fy_token_list *fytl, struct fy_token *fyt)
{
	if (!fyt)
		return;

	assert(fyt->refs > 0);

	if (--fyt->refs == 0)
		fy_token_free_rl(fytl, fyt);
}

static inline FY_ALWAYS_INLINE struct fy_token *
fy_token_alloc(void)
{
	return fy_token_alloc_rl(NULL);
}

static inline FY_ALWAYS_INLINE void
fy_token_clean(struct fy_token *fyt)
{
	return fy_token_clean_rl(NULL, fyt);
}

static inline FY_ALWAYS_INLINE void
fy_token_free(struct fy_token *fyt)
{
	return fy_token_free_rl(NULL, fyt);
}

static inline FY_ALWAYS_INLINE struct fy_token *
fy_token_ref(struct fy_token *fyt)
{
	/* take care of overflow */
	if (!fyt)
		return NULL;
	assert(fyt->refs + 1 > 0);
	fyt->refs++;

	return fyt;
}

static inline FY_ALWAYS_INLINE void
fy_token_unref(struct fy_token *fyt)
{
	return fy_token_unref_rl(NULL, fyt);
}

static inline void
fy_token_list_unref_all(struct fy_token_list *fytl_tofree)
{
	return fy_token_list_unref_all_rl(NULL, fytl_tofree);
}

/* recycling aware */
struct fy_token *fy_token_vcreate_rl(struct fy_token_list *fytl, enum fy_token_type type, va_list ap);
struct fy_token *fy_token_create_rl(struct fy_token_list *fytl, enum fy_token_type type, ...);

struct fy_token *fy_token_vcreate(enum fy_token_type type, va_list ap);
struct fy_token *fy_token_create(enum fy_token_type type, ...);

static inline struct fy_token *
fy_token_list_vqueue(struct fy_token_list *fytl, enum fy_token_type type, va_list ap)
{
	struct fy_token *fyt;

	fyt = fy_token_vcreate(type, ap);
	if (!fyt)
		return NULL;
	fy_token_list_add_tail(fytl, fyt);
	return fyt;
}

static inline struct fy_token *
fy_token_list_queue(struct fy_token_list *fytl, enum fy_token_type type, ...)
{
	va_list ap;
	struct fy_token *fyt;

	va_start(ap, type);
	fyt = fy_token_list_vqueue(fytl, type, ap);
	va_end(ap);

	return fyt;
}

int fy_tag_token_format_text_length(const struct fy_token *fyt);
const char *fy_tag_token_format_text(const struct fy_token *fyt, char *buf, size_t maxsz);
int fy_token_format_utf8_length(struct fy_token *fyt);

int fy_token_format_text_length(struct fy_token *fyt);
const char *fy_token_format_text(struct fy_token *fyt, char *buf, size_t maxsz);

/* non-parser token methods */
struct fy_atom *fy_token_atom(struct fy_token *fyt);

static inline size_t fy_token_start_pos(struct fy_token *fyt)
{
	const struct fy_mark *start_mark;

	if (!fyt)
		return (size_t)-1;

	start_mark = fy_token_start_mark(fyt);
	return start_mark ? start_mark->input_pos : (size_t)-1;
}

static inline size_t fy_token_end_pos(struct fy_token *fyt)
{
	const struct fy_mark *end_mark;

	if (!fyt)
		return (size_t)-1;

	end_mark = fy_token_end_mark(fyt);
	return end_mark ? end_mark->input_pos : (size_t)-1;
}

static inline int fy_token_start_line(struct fy_token *fyt)
{
	const struct fy_mark *start_mark;

	if (!fyt)
		return -1;

	start_mark = fy_token_start_mark(fyt);
	return start_mark ? start_mark->line : -1;
}

static inline int fy_token_start_column(struct fy_token *fyt)
{
	const struct fy_mark *start_mark;

	if (!fyt)
		return -1;

	start_mark = fy_token_start_mark(fyt);
	return start_mark ? start_mark->column : -1;
}

static inline int fy_token_end_line(struct fy_token *fyt)
{
	const struct fy_mark *end_mark;

	if (!fyt)
		return -1;

	end_mark = fy_token_end_mark(fyt);
	return end_mark ? end_mark->line : -1;
}

static inline int fy_token_end_column(struct fy_token *fyt)
{
	const struct fy_mark *end_mark;

	if (!fyt)
		return -1;

	end_mark = fy_token_end_mark(fyt);
	return end_mark ? end_mark->column : -1;
}

static inline bool fy_token_is_multiline(struct fy_token *fyt)
{
	const struct fy_mark *start_mark, *end_mark;

	if (!fyt)
		return false;

	start_mark = fy_token_start_mark(fyt);
	end_mark = fy_token_end_mark(fyt);
	return start_mark && end_mark ? end_mark->line > start_mark->line : false;
}

const char *fy_token_get_direct_output(struct fy_token *fyt, size_t *sizep);

static inline struct fy_input *fy_token_get_input(struct fy_token *fyt)
{
	return fyt ? fyt->handle.fyi : NULL;
}

static inline enum fy_atom_style fy_token_atom_style(struct fy_token *fyt)
{
	if (!fyt)
		return FYAS_PLAIN;

	if (fyt->type == FYTT_TAG)
		return FYAS_URI;

	return fyt->handle.style;
}

static inline bool fy_token_atom_json_mode(struct fy_token *fyt)
{
	if (!fyt)
		return false;

	return fy_atom_json_mode(&fyt->handle);
}

static inline enum fy_lb_mode fy_token_atom_lb_mode(struct fy_token *fyt)
{
	if (!fyt)
		return fylb_cr_nl;

	return fy_atom_lb_mode(&fyt->handle);
}

static inline enum fy_flow_ws_mode fy_token_atom_flow_ws_mode(struct fy_token *fyt)
{
	if (!fyt)
		return fyfws_space_tab;

	return fy_atom_flow_ws_mode(&fyt->handle);
}

static inline bool fy_token_is_lb(struct fy_token *fyt, int c)
{
	if (!fyt)
		return false;

	return fy_atom_is_lb(&fyt->handle, c);
}

static inline bool fy_token_is_flow_ws(struct fy_token *fyt, int c)
{
	if (!fyt)
		return false;

	return fy_atom_is_flow_ws(&fyt->handle, c);
}

#define FYTTAF_HAS_LB			FY_BIT(0)
#define FYTTAF_HAS_WS			FY_BIT(1)
#define FYTTAF_HAS_CONSECUTIVE_LB	FY_BIT(2)
#define FYTTAF_HAS_CONSECUTIVE_WS	FY_BIT(4)
#define FYTTAF_EMPTY			FY_BIT(5)
#define FYTTAF_CAN_BE_SIMPLE_KEY	FY_BIT(6)
#define FYTTAF_DIRECT_OUTPUT		FY_BIT(7)
#define FYTTAF_NO_TEXT_TOKEN		FY_BIT(8)
#define FYTTAF_TEXT_TOKEN		FY_BIT(9)
#define FYTTAF_CAN_BE_PLAIN		FY_BIT(10)
#define FYTTAF_CAN_BE_SINGLE_QUOTED	FY_BIT(11)
#define FYTTAF_CAN_BE_DOUBLE_QUOTED	FY_BIT(12)
#define FYTTAF_CAN_BE_LITERAL		FY_BIT(13)
#define FYTTAF_CAN_BE_FOLDED		FY_BIT(14)
#define FYTTAF_CAN_BE_PLAIN_FLOW	FY_BIT(15)
#define FYTTAF_QUOTE_AT_0		FY_BIT(16)
#define FYTTAF_CAN_BE_UNQUOTED_PATH_KEY	FY_BIT(17)

int fy_token_text_analyze(struct fy_token *fyt);

unsigned int fy_analyze_scalar_content(const char *data, size_t size,
		bool json_mode, enum fy_lb_mode lb_mode, enum fy_flow_ws_mode fws_mode);

/* must be freed */
char *fy_token_debug_text(struct fy_token *fyt);

#define fy_token_debug_text_a(_fyt, _res) \
	do { \
		struct fy_token *__fyt = (_fyt); \
		char *_buf, *_rbuf = ""; \
		size_t _len; \
		_buf = fy_token_debug_text(__fyt); \
		if (_buf) { \
			_len = strlen(_buf); \
			_rbuf = FY_ALLOCA(_len + 1); \
			memcpy(_rbuf, _buf, _len + 1); \
			free(_buf); \
		} \
		*(_res) = _rbuf; \
	} while(false)

int fy_token_memcmp(struct fy_token *fyt, const void *ptr, size_t len);
int fy_token_strcmp(struct fy_token *fyt, const char *str);
int fy_token_cmp(struct fy_token *fyt1, struct fy_token *fyt2);

struct fy_token_iter {
	struct fy_token *fyt;
	struct fy_iter_chunk ic;	/* direct mode */
	struct fy_atom_iter atom_iter;
	int unget_c;
};

void fy_token_iter_start(struct fy_token *fyt, struct fy_token_iter *iter);
void fy_token_iter_finish(struct fy_token_iter *iter);

const char *fy_tag_token_get_directive_handle(struct fy_token *fyt, size_t *td_handle_sizep);
const char *fy_tag_token_get_directive_prefix(struct fy_token *fyt, size_t *td_prefix_sizep);

static inline bool fy_token_is_number(struct fy_token *fyt)
{
	struct fy_atom *atom;

	if (!fyt || fyt->type != FYTT_SCALAR || fyt->scalar.style != FYSS_PLAIN)
		return false;
	atom = fy_token_atom(fyt);
	if (!atom)
		return false;
	return fy_atom_is_number(atom);
}

struct fy_atom *fy_token_comment_handle(struct fy_token *fyt, enum fy_comment_placement placement, bool alloc);
bool fy_token_has_any_comment(struct fy_token *fyt);

const char *fy_token_get_scalar_path_key(struct fy_token *fyt, size_t *lenp);
size_t fy_token_get_scalar_path_key_length(struct fy_token *fyt);
const char *fy_token_get_scalar_path_key0(struct fy_token *fyt);

struct fy_atom *fy_token_comment_handle(struct fy_token *fyt, enum fy_comment_placement placement, bool alloc);

static inline FY_ALWAYS_INLINE enum fy_scalar_style
fy_token_scalar_style_inline(struct fy_token *fyt)
{
	if (!fyt || fyt->type != FYTT_SCALAR)
		return FYSS_PLAIN;

	if (fyt->type == FYTT_SCALAR)
		return fyt->scalar.style;

	return FYSS_PLAIN;
}

static inline FY_ALWAYS_INLINE enum fy_token_type
fy_token_get_type_inline(struct fy_token *fyt)
{
	return fyt ? fyt->type : FYTT_NONE;
}

#endif
