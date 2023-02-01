/*
 * fy-input.h - YAML input methods
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_INPUT_H
#define FY_INPUT_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include <libfyaml.h>

#include "fy-utils.h"
#include "fy-typelist.h"
#include "fy-ctype.h"

struct fy_atom;
struct fy_parser;

enum fy_input_type {
	fyit_file,
	fyit_stream,
	fyit_memory,
	fyit_alloc,
	fyit_callback,
	fyit_fd,
};

struct fy_input_cfg {
	enum fy_input_type type;
	void *userdata;
	size_t chunk;
	bool ignore_stdio : 1;
	bool no_fclose_fp : 1;
	bool no_close_fd : 1;
	union {
		struct {
			const char *filename;
		} file;
		struct {
			const char *name;
			FILE *fp;
		} stream;
		struct {
			const void *data;
			size_t size;
		} memory;
		struct {
			void *data;
			size_t size;
		} alloc;
		struct {
			/* negative return is error, 0 is EOF */
			ssize_t (*input)(void *user, void *buf, size_t count);
		} callback;
		struct {
			int fd;
		} fd;
	};
};

enum fy_input_state {
	FYIS_NONE,
	FYIS_QUEUED,
	FYIS_PARSE_IN_PROGRESS,
	FYIS_PARSED,
};

FY_TYPE_FWD_DECL_LIST(input);
struct fy_input {
	struct fy_list_head node;
	enum fy_input_state state;
	struct fy_input_cfg cfg;
	int refs;		/* number of referers */
	char *name;
	void *buffer;		/* when the file can't be mmaped */
	uint64_t generation;
	size_t allocated;
	size_t read;
	size_t chunk;
	size_t chop;
	FILE *fp;		/* FILE* for the input if it exists */
	int fd;			/* fd for file and stream */
	size_t length;		/* length of file */
	void *addr;		/* mmaped for files, allocated for streams */
	bool eof : 1;		/* got EOF */
	bool err : 1;		/* got an error */

	/* propagated */
	bool json_mode;
	enum fy_lb_mode lb_mode;
	enum fy_flow_ws_mode fws_mode;
};
FY_TYPE_DECL_LIST(input);

static inline const void *fy_input_start(const struct fy_input *fyi)
{
	const void *ptr = NULL;

	switch (fyi->cfg.type) {
	case fyit_file:
		if (fyi->addr) {
			ptr = fyi->addr;
			break;
		}
		/* fall-through */

	case fyit_stream:
	case fyit_callback:
		ptr = fyi->buffer;
		break;

	case fyit_memory:
		ptr = fyi->cfg.memory.data;
		break;

	case fyit_alloc:
		ptr = fyi->cfg.alloc.data;
		break;

	default:
		break;
	}
	assert(ptr);
	return ptr;
}

static inline size_t fy_input_size(const struct fy_input *fyi)
{
	size_t size;

	switch (fyi->cfg.type) {
	case fyit_file:
		if (fyi->addr) {
			size = fyi->length;
			break;
		}
		/* fall-through */

	case fyit_stream:
	case fyit_callback:
		size = fyi->read;
		break;

	case fyit_memory:
		size = fyi->cfg.memory.size;
		break;

	case fyit_alloc:
		size = fyi->cfg.alloc.size;
		break;

	default:
		size = 0;
		break;
	}
	return size;
}

struct fy_input *fy_input_alloc(void);
void fy_input_free(struct fy_input *fyi);

static inline enum fy_input_state fy_input_get_state(struct fy_input *fyi)
{
	return fyi->state;
}

struct fy_input *fy_input_create(const struct fy_input_cfg *fyic);

const char *fy_input_get_filename(struct fy_input *fyi);

struct fy_input *fy_input_from_data(const char *data, size_t size,
				    struct fy_atom *handle, bool simple);
struct fy_input *fy_input_from_malloc_data(char *data, size_t size,
					   struct fy_atom *handle, bool simple);

void fy_input_close(struct fy_input *fyi);

static inline struct fy_input *
fy_input_ref(struct fy_input *fyi)
{
	if (!fyi)
		return NULL;


	assert(fyi->refs + 1 > 0);

	fyi->refs++;

	return fyi;
}

static inline void
fy_input_unref(struct fy_input *fyi)
{
	if (!fyi)
		return;

	assert(fyi->refs > 0);

	if (fyi->refs == 1)
		fy_input_free(fyi);
	else
		fyi->refs--;
}

struct fy_reader;

enum fy_reader_mode {
	fyrm_yaml,
	fyrm_json,
	fyrm_yaml_1_1,	/* yaml 1.1 mode */
};

struct fy_reader_ops {
	struct fy_diag *(*get_diag)(struct fy_reader *fyr);
	int (*file_open)(struct fy_reader *fyr, const char *filename);
};

struct fy_reader_input_cfg {
	bool disable_mmap_opt;
};

struct fy_reader {
	const struct fy_reader_ops *ops;
	enum fy_reader_mode mode;

	struct fy_reader_input_cfg current_input_cfg;
	struct fy_input *current_input;

	size_t this_input_start;	/* this input start */
	size_t current_input_pos;	/* from start of input */
	const void *current_ptr;	/* current pointer into the buffer */
	int current_c;			/* current utf8 character at current_ptr (-1 if not cached) */
	int current_w;			/* current utf8 character width */
	size_t current_left;		/* currently left characters into the buffer */

	int line;			/* always on input */
	int column;

	int tabsize;			/* very experimental tab size for indent purposes */

	struct fy_diag *diag;

	/* decoded mode variables; update when changing modes */
	bool json_mode;
	enum fy_lb_mode lb_mode;
	enum fy_flow_ws_mode fws_mode;
};

void fy_reader_reset(struct fy_reader *fyr);
void fy_reader_setup(struct fy_reader *fyr, const struct fy_reader_ops *ops);
void fy_reader_cleanup(struct fy_reader *fyr);

int fy_reader_input_open(struct fy_reader *fyr, struct fy_input *fyi, const struct fy_reader_input_cfg *icfg);
int fy_reader_input_done(struct fy_reader *fyr);
int fy_reader_input_scan_token_mark_slow_path(struct fy_reader *fyr);

static inline bool
fy_reader_input_chop_active(struct fy_reader *fyr)
{
	struct fy_input *fyi;

	assert(fyr);

	fyi = fyr->current_input;
	assert(fyi);

	if (!fyi->chop)
		return false;

	switch (fyi->cfg.type) {
	case fyit_file:
		return !fyi->addr && fyi->fp;	/* non-mmap mode */

	case fyit_stream:
	case fyit_callback:
		return true;

	default:
		/* all the others do not support chop */
		break;
	}

	return false;
}

static inline int
fy_reader_input_scan_token_mark(struct fy_reader *fyr)
{
	/* don't chop until ready */
	if (!fy_reader_input_chop_active(fyr) ||
	    fyr->current_input->chop > fyr->current_input_pos)
		return 0;

	return fy_reader_input_scan_token_mark_slow_path(fyr);
}

const void *fy_reader_ptr_slow_path(struct fy_reader *fyr, size_t *leftp);
const void *fy_reader_ensure_lookahead_slow_path(struct fy_reader *fyr, size_t size, size_t *leftp);

void fy_reader_apply_mode(struct fy_reader *fyr);

static FY_ALWAYS_INLINE inline enum fy_reader_mode
fy_reader_get_mode(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->mode;
}

static FY_ALWAYS_INLINE inline void
fy_reader_set_mode(struct fy_reader *fyr, enum fy_reader_mode mode)
{
	assert(fyr);
	fyr->mode = mode;
	fy_reader_apply_mode(fyr);
}

static FY_ALWAYS_INLINE inline struct fy_input *
fy_reader_current_input(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->current_input;
}

static FY_ALWAYS_INLINE inline uint64_t
fy_reader_current_input_generation(const struct fy_reader *fyr)
{
	assert(fyr);
	assert(fyr->current_input);
	return fyr->current_input->generation;
}

static FY_ALWAYS_INLINE inline int
fy_reader_column(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->column;
}

static FY_ALWAYS_INLINE inline int
fy_reader_tabsize(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->tabsize;
}

static FY_ALWAYS_INLINE inline int
fy_reader_line(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->line;
}

/* force new line at the end of stream */
static inline void fy_reader_stream_end(struct fy_reader *fyr)
{
	assert(fyr);

	/* force new line */
	if (fyr->column) {
		fyr->column = 0;
		fyr->line++;
	}
}

static FY_ALWAYS_INLINE inline void
fy_reader_get_mark(struct fy_reader *fyr, struct fy_mark *fym)
{
	assert(fyr);
	fym->input_pos = fyr->current_input_pos;
	fym->line = fyr->line;
	fym->column = fyr->column;
}

static FY_ALWAYS_INLINE inline const void *
fy_reader_ptr(struct fy_reader *fyr, size_t *leftp)
{
	if (fyr->current_ptr) {
		if (leftp)
			*leftp = fyr->current_left;
		return fyr->current_ptr;
	}

	return fy_reader_ptr_slow_path(fyr, leftp);
}

static FY_ALWAYS_INLINE inline bool
fy_reader_json_mode(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->json_mode;
}

static FY_ALWAYS_INLINE inline enum fy_lb_mode
fy_reader_lb_mode(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->lb_mode;
}

static FY_ALWAYS_INLINE inline enum fy_flow_ws_mode
fy_reader_flow_ws_mode(const struct fy_reader *fyr)
{
	assert(fyr);
	return fyr->fws_mode;
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_lb(const struct fy_reader *fyr, int c)
{
	return fy_is_lb_m(c, fy_reader_lb_mode(fyr));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_lbz(const struct fy_reader *fyr, int c)
{
	return fy_is_lbz_m(c, fy_reader_lb_mode(fyr));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_blankz(const struct fy_reader *fyr, int c)
{
	return fy_is_blankz_m(c, fy_reader_lb_mode(fyr));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_generic_lb(const struct fy_reader *fyr, int c)
{
	return fy_is_generic_lb_m(c, fy_reader_lb_mode(fyr));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_generic_lbz(const struct fy_reader *fyr, int c)
{
	return fy_is_generic_lbz_m(c, fy_reader_lb_mode(fyr));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_generic_blankz(const struct fy_reader *fyr, int c)
{
	return fy_is_generic_blankz_m(c, fy_reader_lb_mode(fyr));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_flow_ws(const struct fy_reader *fyr, int c)
{
	return fy_is_flow_ws_m(c, fy_reader_flow_ws_mode(fyr));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_flow_blank(const struct fy_reader *fyr, int c)
{
	return fy_reader_is_flow_ws(fyr, c);	/* same */
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_flow_blankz(const struct fy_reader *fyr, int c)
{
	return fy_is_flow_ws_m(c, fy_reader_flow_ws_mode(fyr)) ||
	       fy_is_generic_lbz_m(c, fy_reader_lb_mode(fyr));
}

static FY_ALWAYS_INLINE inline const void *
fy_reader_ensure_lookahead(struct fy_reader *fyr, size_t size, size_t *leftp)
{
	if (fyr->current_ptr && fyr->current_left >= size) {
		if (leftp)
			*leftp = fyr->current_left;
		return fyr->current_ptr;
	}
	return fy_reader_ensure_lookahead_slow_path(fyr, size, leftp);
}

/* compare string at the current point (n max) */
static inline int
fy_reader_strncmp(struct fy_reader *fyr, const char *str, size_t n)
{
	const char *p;
	int ret;

	assert(fyr);
	p = fy_reader_ensure_lookahead(fyr, n, NULL);
	if (!p)
		return -1;
	ret = strncmp(p, str, n);
	return ret ? 1 : 0;
}

static FY_ALWAYS_INLINE inline int
fy_reader_peek_at_offset(struct fy_reader *fyr, size_t offset)
{
	const uint8_t *p;
	size_t left;
	int w;

	assert(fyr);
	if (offset == 0 && fyr->current_c >= 0)
		return fyr->current_c;

	/* ensure that the first octet at least is pulled in */
	p = fy_reader_ensure_lookahead(fyr, offset + 1, &left);
	if (!p)
		return FYUG_EOF;

	/* get width by first octet */
	w = fy_utf8_width_by_first_octet(p[offset]);
	if (!w)
		return FYUG_INV;

	/* make sure that there's enough to cover the utf8 width */
	if (offset + w > left) {
		p = fy_reader_ensure_lookahead(fyr, offset + w, &left);
		if (!p)
			return FYUG_PARTIAL;
	}

	return fy_utf8_get(p + offset, left - offset, &w);
}

static FY_ALWAYS_INLINE inline int
fy_reader_peek_at_internal(struct fy_reader *fyr, int pos, ssize_t *offsetp)
{
	int i, c;
	size_t offset;

	assert(fyr);
	if (!offsetp || *offsetp < 0) {
		for (i = 0, offset = 0; i < pos; i++, offset += fy_utf8_width(c)) {
			c = fy_reader_peek_at_offset(fyr, offset);
			if (c < 0)
				return c;
		}
	} else
		offset = (size_t)*offsetp;

	c = fy_reader_peek_at_offset(fyr, offset);

	if (offsetp)
		*offsetp = offset + fy_utf8_width(c);

	return c;
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_blank_at_offset(struct fy_reader *fyr, size_t offset)
{
	return fy_is_blank(fy_reader_peek_at_offset(fyr, offset));
}

static FY_ALWAYS_INLINE inline bool
fy_reader_is_blankz_at_offset(struct fy_reader *fyr, size_t offset)
{
	return fy_reader_is_blankz(fyr, fy_reader_peek_at_offset(fyr, offset));
}

static FY_ALWAYS_INLINE inline int
fy_reader_peek_at(struct fy_reader *fyr, int pos)
{
	return fy_reader_peek_at_internal(fyr, pos, NULL);
}

static FY_ALWAYS_INLINE inline int
fy_reader_peek(struct fy_reader *fyr)
{
	if (fyr->current_c >= 0)
		return fyr->current_c;

	return fy_reader_peek_at_offset(fyr, 0);
}

static FY_ALWAYS_INLINE inline const void *
fy_reader_peek_block(struct fy_reader *fyr, size_t *lenp)
{
	const void *p;

	/* try to pull at least one utf8 character usually */
	p = fy_reader_ensure_lookahead(fyr, 4, lenp);

	/* not a utf8 character available? try a single byte */
	if (!p)
		p = fy_reader_ensure_lookahead(fyr, 1, lenp);
	if (!*lenp)
		p = NULL;
	return p;
}

static FY_ALWAYS_INLINE inline void
fy_reader_advance_octets(struct fy_reader *fyr, size_t advance)
{
	assert(fyr);
	assert(fyr->current_left >= advance);

	fyr->current_input_pos += advance;
	fyr->current_ptr = (char *)fyr->current_ptr + advance;
	fyr->current_left -= advance;

	fyr->current_c = fy_utf8_get(fyr->current_ptr, fyr->current_left, &fyr->current_w);
}

void fy_reader_advance_slow_path(struct fy_reader *fyr, int c);

static FY_ALWAYS_INLINE inline void
fy_reader_advance_printable_ascii(struct fy_reader *fyr, int c)
{
	assert(fyr);
	fy_reader_advance_octets(fyr, 1);
	fyr->column++;
}

static FY_ALWAYS_INLINE inline void
fy_reader_advance(struct fy_reader *fyr, int c)
{
	if (fy_utf8_is_printable_ascii(c))
		fy_reader_advance_printable_ascii(fyr, c);
	else
		fy_reader_advance_slow_path(fyr, c);
}

static FY_ALWAYS_INLINE inline void
fy_reader_advance_ws(struct fy_reader *fyr, int c)
{
	/* skip this character */
	fy_reader_advance_octets(fyr, fy_utf8_width(c));

	if (fyr->tabsize && fy_is_tab(c))
		fyr->column += (fyr->tabsize - (fyr->column % fyr->tabsize));
	else
		fyr->column++;
}

static FY_ALWAYS_INLINE inline void
fy_reader_advance_space(struct fy_reader *fyr)
{
	fy_reader_advance_octets(fyr, 1);
	fyr->column++;
}

static FY_ALWAYS_INLINE inline int
fy_reader_get(struct fy_reader *fyr)
{
	int value;

	value = fy_reader_peek(fyr);
	if (value < 0)
		return value;

	fy_reader_advance(fyr, value);

	return value;
}

static FY_ALWAYS_INLINE inline int
fy_reader_advance_by(struct fy_reader *fyr, int count)
{
	int i, c;

	for (i = 0; i < count; i++) {
		c = fy_reader_get(fyr);
		if (c < 0)
			break;
	}
	return i ? i : -1;
}

/* compare string at the current point */
static inline bool
fy_reader_strcmp(struct fy_reader *fyr, const char *str)
{
	return fy_reader_strncmp(fyr, str, strlen(str));
}

#endif
