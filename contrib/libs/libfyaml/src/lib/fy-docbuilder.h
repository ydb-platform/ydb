/*
 * fy-docbuilder.h - YAML document builder internal header file
 *
 * Copyright (c) 2022 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_DOCBUILDER_H
#define FY_DOCBUILDER_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-doc.h"

enum fy_document_builder_state {
	FYDBS_NODE,
	FYDBS_MAP_KEY,
	FYDBS_MAP_VAL,
	FYDBS_SEQ,
};

struct fy_document_builder_ctx {
	enum fy_document_builder_state s;
	struct fy_node *fyn;
	struct fy_node_pair *fynp;	/* for mapping */
};

struct fy_document_builder_cfg {
	struct fy_parse_cfg parse_cfg;
	void *userdata;
	struct fy_diag *diag;
};

struct fy_document_builder {
	struct fy_document_builder_cfg cfg;
	struct fy_document *fyd;
	bool single_mode;
	bool in_stream;
	bool doc_done;
	unsigned int next;
	unsigned int alloc;
	unsigned int max_depth;
	struct fy_document_builder_ctx *stack;
};

struct fy_document_builder *
fy_document_builder_create(const struct fy_document_builder_cfg *cfg);

void
fy_document_builder_reset(struct fy_document_builder *fydb);

void
fy_document_builder_destroy(struct fy_document_builder *fydb);

struct fy_document *
fy_document_builder_get_document(struct fy_document_builder *fydb);

bool
fy_document_builder_is_in_stream(struct fy_document_builder *fydb);

bool
fy_document_builder_is_in_document(struct fy_document_builder *fydb);

bool
fy_document_builder_is_document_complete(struct fy_document_builder *fydb);

struct fy_document *
fy_document_builder_take_document(struct fy_document_builder *fydb);

struct fy_document *
fy_document_builder_peek_document(struct fy_document_builder *fydb);

void
fy_document_builder_set_in_stream(struct fy_document_builder *fydb);

int
fy_document_builder_set_in_document(struct fy_document_builder *fydb, struct fy_document_state *fyds, bool single);

int
fy_document_builder_process_event(struct fy_document_builder *fydb, struct fy_eventp *fyep);

struct fy_document *
fy_document_builder_load_document(struct fy_document_builder *fydb,
				  struct fy_parser *fyp);

struct fy_document *
fy_parse_load_document_with_builder(struct fy_parser *fyp);

#endif
