/*
 * fy-atom.h - internal YAML atom methods
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */

#ifndef FY_ATOM_H
#define FY_ATOM_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-list.h"
#include "fy-input.h"

struct fy_reader;
struct fy_input;
struct fy_node;

enum fy_atom_style {
	/* YAML atoms */
	FYAS_PLAIN,
	FYAS_SINGLE_QUOTED,
	FYAS_DOUBLE_QUOTED,
	FYAS_LITERAL,
	FYAS_FOLDED,
	FYAS_URI,	/* special style for URIs */
	FYAS_DOUBLE_QUOTED_MANUAL,
	FYAS_COMMENT	/* (possibly multi line) comment */
};

static inline bool fy_atom_style_is_quoted(enum fy_atom_style style)
{
	return style == FYAS_SINGLE_QUOTED || style == FYAS_DOUBLE_QUOTED;
}

static inline bool fy_atom_style_is_block(enum fy_atom_style style)
{
	return style == FYAS_LITERAL || style == FYAS_FOLDED;
}

enum fy_atom_chomp {
	FYAC_STRIP,
	FYAC_CLIP,
	FYAC_KEEP,
};

struct fy_atom {
	struct fy_mark start_mark;
	struct fy_mark end_mark;
	size_t storage_hint;	/* guaranteed to fit in this amount of bytes */
	struct fy_input *fyi;	/* input on which atom is on */
	uint64_t fyi_generation;	/* to detect reallocs */
	unsigned int increment;
	union {
		uint64_t tozero;			/* fast way to zero everything here */
		struct {
			/* save a little bit of space with bitfields */
			enum fy_atom_style style : 8;	/* note that it's a big perf win for bytes */
			enum fy_atom_chomp chomp : 8;
			unsigned int tabsize : 8;
			enum fy_lb_mode lb_mode : 1;
			enum fy_flow_ws_mode fws_mode : 1;
			bool direct_output : 1;		/* can directly output */
			bool storage_hint_valid : 1;
			bool empty : 1;			/* atom contains whitespace and linebreaks only if length > 0 */
			bool has_lb : 1;		/* atom contains at least one linebreak */
			bool has_ws : 1;		/* atom contains at least one whitespace */
			bool starts_with_ws : 1;	/* atom starts with whitespace */
			bool starts_with_lb : 1;	/* atom starts with linebreak */
			bool ends_with_ws : 1;		/* atom ends with whitespace */
			bool ends_with_lb : 1;		/* atom ends with linebreak */
			bool trailing_lb : 1;		/* atom ends with trailing linebreaks > 1 */
			bool size0 : 1;			/* atom contains absolutely nothing */
			bool valid_anchor : 1;		/* atom is a valid anchor */
			bool json_mode : 1;		/* atom was read in json mode */
			bool ends_with_eof : 1;		/* atom ends at EOF of input */
		};
	};
};

static inline bool fy_atom_is_set(const struct fy_atom *atom)
{
	return atom && atom->fyi;
}

static inline void fy_atom_reset(struct fy_atom *atom)
{
	if (atom)
		atom->fyi = NULL;
}

static inline bool fy_atom_json_mode(struct fy_atom *handle)
{
	if (!handle)
		return false;

	return handle->json_mode;
}

static inline enum fy_lb_mode fy_atom_lb_mode(struct fy_atom *handle)
{
	if (!handle)
		return fylb_cr_nl;

	return handle->lb_mode;
}

static inline enum fy_flow_ws_mode fy_atom_flow_ws_mode(struct fy_atom *handle)
{
	if (!handle)
		return fyfws_space_tab;

	return handle->fws_mode;
}

/* all atoms are scalars so... */
static inline bool fy_atom_is_lb(struct fy_atom *handle, int c)
{
	return fy_is_generic_lb_m(c, fy_atom_lb_mode(handle));
}

static inline bool fy_atom_is_flow_ws(struct fy_atom *handle, int c)
{
	return fy_is_flow_ws_m(c, fy_atom_flow_ws_mode(handle));
}

int fy_atom_format_text_length(struct fy_atom *atom);
const char *fy_atom_format_text(struct fy_atom *atom, char *buf, size_t maxsz);

int fy_atom_format_utf8_length(struct fy_atom *atom);

static inline void
fy_reader_fill_atom_start(struct fy_reader *fyr, struct fy_atom *handle)
{
	/* start mark */
	fy_reader_get_mark(fyr, &handle->start_mark);
	handle->fyi = fy_reader_current_input(fyr);
	handle->fyi_generation = fy_reader_current_input_generation(fyr);

	handle->increment = 0;
	handle->tozero = 0;

	/* note that handle->data may be zero for empty input */
}

static inline void
fy_reader_fill_atom_end_at(struct fy_reader *fyr, struct fy_atom *handle, struct fy_mark *end_mark)
{
	if (end_mark)
		handle->end_mark = *end_mark;
	else
		fy_reader_get_mark(fyr, &handle->end_mark);

	/* default is plain, modify at return */
	handle->style = FYAS_PLAIN;
	handle->chomp = FYAC_CLIP;
	/* by default we don't do storage hints, it's the job of the caller */
	handle->storage_hint = 0;
	handle->storage_hint_valid = false;
	handle->tabsize = fy_reader_tabsize(fyr);
	handle->json_mode = fy_reader_json_mode(fyr);
	handle->lb_mode = fy_reader_lb_mode(fyr);
	handle->fws_mode = fy_reader_flow_ws_mode(fyr);
}

static inline void
fy_reader_fill_atom_end(struct fy_reader *fyr, struct fy_atom *handle)
{
	fy_reader_fill_atom_end_at(fyr, handle, NULL);
}

static inline void
fy_atom_reset_storage_hints(struct fy_atom *handle)
{
	handle->storage_hint = 0;
	handle->storage_hint_valid = false;
}

struct fy_atom *fy_reader_fill_atom(struct fy_reader *fyr, int advance, struct fy_atom *handle);
struct fy_atom *fy_reader_fill_atom_mark(struct fy_reader *fyr, const struct fy_mark *start_mark,
					 const struct fy_mark *end_mark, struct fy_atom *handle);
struct fy_atom *fy_reader_fill_atom_at(struct fy_reader *fyr, int advance, int count, struct fy_atom *handle);

#define fy_reader_fill_atom_a(_fyr, _advance)  fy_reader_fill_atom((_fyr), (_advance), FY_ALLOCA(sizeof(struct fy_atom)))

struct fy_atom *fy_fill_node_atom(struct fy_node *fyn, struct fy_atom *handle);

#define fy_fill_node_atom_a(_fyn)  fy_fill_node_atom((_fyn), FY_ALLOCA(sizeof(struct fy_atom)))

struct fy_atom_iter_line_info {
	const char *start;
	const char *end;
	const char *nws_start;
	const char *nws_end;
	const char *chomp_start;
	bool empty : 1;
	bool trailing_breaks_ws : 1;
	bool first : 1;		/* first */
	bool last : 1;		/* last (only ws/lb afterwards */
	bool final : 1;		/* the final iterator */
	bool indented : 1;
	bool lb_end : 1;
	bool need_nl : 1;
	bool need_sep : 1;
	bool ends_with_backslash : 1;	/* last ended in \\ */
	size_t trailing_ws;
	size_t trailing_breaks;
	size_t start_ws, end_ws;
	const char *s;
	const char *e;
	int actual_lb;		/* the line break */
	const char *s_tb;	/* start of trailing breaks run */
	const char *e_tb;	/* end of trailing breaks run */
};

struct fy_atom_iter_chunk {
	struct fy_iter_chunk ic;
	/* note that it is guaranteed for copied chunks to be
	 * less or equal to 10 characters (the maximum digitbuf
	 * for double quoted escapes */
	char inplace_buf[10];	/* small copies in place */
};

#define NR_STARTUP_CHUNKS	8
#define SZ_STARTUP_COPY_BUFFER	32

struct fy_atom_iter {
	const struct fy_atom *atom;
	const char *s, *e;
	unsigned int chomp;
	int tabsize;
	bool single_line : 1;
	bool dangling_end_quote : 1;
	bool last_ends_with_backslash : 1;
	bool empty : 1;
	bool current : 1;
	bool done : 1;	/* last iteration (for block styles) */
	struct fy_atom_iter_line_info li[2];
	unsigned int alloc;
	unsigned int top;
	unsigned int read;
	struct fy_atom_iter_chunk *chunks;
	struct fy_atom_iter_chunk startup_chunks[NR_STARTUP_CHUNKS];
	int unget_c;
};

void fy_atom_iter_start(const struct fy_atom *atom, struct fy_atom_iter *iter);
void fy_atom_iter_finish(struct fy_atom_iter *iter);
const struct fy_iter_chunk *fy_atom_iter_peek_chunk(struct fy_atom_iter *iter);
const struct fy_iter_chunk *fy_atom_iter_chunk_next(struct fy_atom_iter *iter, const struct fy_iter_chunk *curr, int *errp);
void fy_atom_iter_advance(struct fy_atom_iter *iter, size_t len);

struct fy_atom_iter *fy_atom_iter_create(const struct fy_atom *atom);
void fy_atom_iter_destroy(struct fy_atom_iter *iter);
ssize_t fy_atom_iter_read(struct fy_atom_iter *iter, void *buf, size_t count);
int fy_atom_iter_getc(struct fy_atom_iter *iter);
int fy_atom_iter_ungetc(struct fy_atom_iter *iter, int c);
int fy_atom_iter_peekc(struct fy_atom_iter *iter);
int fy_atom_iter_utf8_get(struct fy_atom_iter *iter);
int fy_atom_iter_utf8_quoted_get(struct fy_atom_iter *iter, size_t *lenp, uint8_t *buf);
int fy_atom_iter_utf8_unget(struct fy_atom_iter *iter, int c);
int fy_atom_iter_utf8_peek(struct fy_atom_iter *iter);

int fy_atom_memcmp(struct fy_atom *atom, const void *ptr, size_t len);
int fy_atom_strcmp(struct fy_atom *atom, const char *str);
bool fy_atom_is_number(struct fy_atom *atom);
int fy_atom_cmp(struct fy_atom *atom1, struct fy_atom *atom2);

static inline const char *fy_atom_data(const struct fy_atom *atom)
{
	if (!atom)
		return NULL;

	return (char *)fy_input_start(atom->fyi) + atom->start_mark.input_pos;
}

static inline size_t fy_atom_size(const struct fy_atom *atom)
{
	if (!atom)
		return 0;

	return atom->end_mark.input_pos - atom->start_mark.input_pos;
}

static inline bool fy_plain_atom_streq(const struct fy_atom *atom, const char *str)
{
	size_t size = strlen(str);

	if (!atom || !str || atom->style != FYAS_PLAIN || fy_atom_size(atom) != size)
		return false;

	return !memcmp(str, fy_atom_data(atom), size);
}

struct fy_raw_line {
	int lineno;
	const char *line_start;
	size_t line_len;
	size_t line_len_lb;
	size_t line_count;
	const char *content_start;
	size_t content_len;
	size_t content_start_count;
	size_t content_count;
	int content_start_col;
	int content_start_col8;	/* this is the tab 8 */
	int content_end_col;
	int content_end_col8;
};

struct fy_atom_raw_line_iter {
	const struct fy_atom *atom;
	const char *is, *ie;	/* input start, end */
	const char *as, *ae;	/* atom start, end */
	const char *rs;
	struct fy_raw_line line;
};

void fy_atom_raw_line_iter_start(const struct fy_atom *atom,
			     struct fy_atom_raw_line_iter *iter);
void fy_atom_raw_line_iter_finish(struct fy_atom_raw_line_iter *iter);

const struct fy_raw_line *
fy_atom_raw_line_iter_next(struct fy_atom_raw_line_iter *iter);

#endif
