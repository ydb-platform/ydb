/*
 * fy-emit.h - internal YAML emitter header
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_EMIT_H
#define FY_EMIT_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>

#include <libfyaml.h>

#include "fy-utf8.h"
#include "fy-event.h"
#include "fy-emit-accum.h"

#define FYEF_WHITESPACE			0x0001
#define FYEF_INDENTATION		0x0002
#define FYEF_OPEN_ENDED			0x0004
#define FYEF_HAD_DOCUMENT_START		0x0008
#define FYEF_HAD_DOCUMENT_END		0x0010
#define FYEF_HAD_DOCUMENT_OUTPUT	0x0020

struct fy_document;
struct fy_emitter;
struct fy_document_state;

enum fy_emitter_state {
	FYES_NONE,		/* when not using the raw emitter interface */
	FYES_STREAM_START,
	FYES_FIRST_DOCUMENT_START,
	FYES_DOCUMENT_START,
	FYES_DOCUMENT_CONTENT,
	FYES_DOCUMENT_END,
	FYES_SEQUENCE_FIRST_ITEM,
	FYES_SEQUENCE_ITEM,
	FYES_MAPPING_FIRST_KEY,
	FYES_MAPPING_KEY,
	FYES_MAPPING_SIMPLE_VALUE,
	FYES_MAPPING_VALUE,
	FYES_END,
};

struct fy_emit_save_ctx {
	bool flow_token : 1;
	bool flow : 1;
	bool empty : 1;
	enum fy_node_style xstyle;
	int old_indent;
	int flags;
	int indent;
	struct fy_token *fyt_last_key;
	struct fy_token *fyt_last_value;
	int s_flags;
	int s_indent;
};

/* internal flags */
#define DDNF_ROOT		0x0001
#define DDNF_SEQ		0x0002
#define DDNF_MAP		0x0004
#define DDNF_SIMPLE		0x0008
#define DDNF_FLOW		0x0010
#define DDNF_INDENTLESS		0x0020
#define DDNF_SIMPLE_SCALAR_KEY	0x0040

struct fy_emitter {
	int line;
	int column;
	int flow_level;
	unsigned int flags;
	bool output_error : 1;
	bool source_json : 1;		/* the source was json */
	bool force_json : 1;		/* force JSON mode unconditionally */
	bool suppress_recycling_force : 1;
	bool suppress_recycling : 1;

	/* current document */
	struct fy_emitter_cfg cfg;	/* yeah, it isn't worth just to save a few bytes */
	struct fy_document *fyd;
	struct fy_document_state *fyds;	/* fyd->fyds when fyd != NULL */
	struct fy_emit_accum ea;
	char ea_inplace_buf[256];	/* the in place accumulator buffer before allocating */
	struct fy_diag *diag;

	/* streaming event mode */
	enum fy_emitter_state state;
	enum fy_emitter_state *state_stack;
	unsigned int state_stack_alloc;
	unsigned int state_stack_top;
	enum fy_emitter_state state_stack_inplace[64];
	struct fy_eventp_list queued_events;
	int s_indent;
	int s_flags;
	struct fy_emit_save_ctx s_sc;
	struct fy_emit_save_ctx *sc_stack;
	unsigned int sc_stack_alloc;
	unsigned int sc_stack_top;
	struct fy_emit_save_ctx sc_stack_inplace[16];

	/* recycled */
	struct fy_eventp_list recycled_eventp;
	struct fy_token_list recycled_token;

	struct fy_eventp_list *recycled_eventp_list;	/* NULL when suppressing */
	struct fy_token_list *recycled_token_list;	/* NULL when suppressing */

	/* for special needs */
	void (*finalizer)(struct fy_emitter *emit);
};

int fy_emit_setup(struct fy_emitter *emit, const struct fy_emitter_cfg *cfg);
void fy_emit_cleanup(struct fy_emitter *emit);

void fy_emit_write(struct fy_emitter *emit, enum fy_emitter_write_type type, const char *str, int len);

static inline bool fy_emit_whitespace(struct fy_emitter *emit)
{
	return !!(emit->flags & FYEF_WHITESPACE);
}

static inline bool fy_emit_indentation(struct fy_emitter *emit)
{
	return !!(emit->flags & FYEF_INDENTATION);
}

static inline bool fy_emit_open_ended(struct fy_emitter *emit)
{
	return !!(emit->flags & FYEF_OPEN_ENDED);
}

static inline void
fy_emit_output_accum(struct fy_emitter *emit, enum fy_emitter_write_type type, struct fy_emit_accum *ea)
{
	const char *text;
	size_t len;

	text = fy_emit_accum_get(ea, &len);
	if (text && len > 0)
		fy_emit_write(emit, type, text, len);
	fy_emit_accum_reset(ea);
}

#endif
